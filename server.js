var _ = require('lodash');
var argv = require('minimist')(process.argv.slice(2));
var http = require('http');
var socketCluster = require('socketcluster-server');
var url = require('url');

var DEFAULT_PORT = 7777;
var DEFAULT_CLUSTER_SCALE_DELAY = 5000;

var RETRY_DELAY = Number(argv.r) || Number(process.env.SCC_STATE_SERVER_RETRY_DELAY) || 2000;
var PORT = Number(argv.p) || Number(process.env.SCC_STATE_SERVER_PORT) || DEFAULT_PORT;
var CLUSTER_SCALE_DELAY = Number(argv.d) || Number(process.env.SCC_STATE_SERVER_SCALE_DELAY) || DEFAULT_CLUSTER_SCALE_DELAY;
var AUTH_KEY = process.env.SCC_AUTH_KEY || null;
var FORWARDED_FOR_HEADER = process.env.FORWARDED_FOR_HEADER || null;
/**
 * Log levels:
 * 3 - log everything
 * 2 - warnings and errors
 * 1 - errors only
 * 0 - log nothing
 */
var LOG_LEVEL;
if (typeof argv.l !== 'undefined') {
  LOG_LEVEL = Number(argv.l);
} else if (typeof process.env.SCC_STATE_LOG_LEVEL !== 'undefined') {
  LOG_LEVEL = Number(process.env.SCC_STATE_LOG_LEVEL);
} else {
  LOG_LEVEL = 3;
}

var httpServer = http.createServer();
var scServer = socketCluster.attach(httpServer);

httpServer.on('request', function (req, res) {
  if (req.url == '/health-check') {
    res.writeHead(200, {'Content-Type': 'text/html'});
    res.end('OK');
  } else {
    res.writeHead(404, {'Content-Type': 'text/html'});
    res.end('Not found');
  }
});

var serverInstanceSockets = {};
var clientInstanceSockets = {};

var getServerClusterState = function () {
  var serverInstances = [];
  _.forOwn(serverInstanceSockets, function (socket) {
    var targetProtocol = socket.instanceSecure ? 'wss' : 'ws';
    var instanceIp;
    if (socket.instanceIpFamily == 'IPv4') {
      instanceIp = socket.instanceIp;
    } else {
      instanceIp = `[${socket.instanceIp}]`;
    }
    var serverURI = `${targetProtocol}://${instanceIp}:${socket.instancePort}`;
    serverInstances.push(serverURI);
  });
  return {
    serverInstances: serverInstances,
    time: Date.now()
  };
};

var clusterResizeTimeout;

var setClusterScaleTimeout = function (callback) {
  // Only the latest scale request counts.
  if (clusterResizeTimeout) {
    clearTimeout(clusterResizeTimeout);
  }
  clusterResizeTimeout = setTimeout(callback, CLUSTER_SCALE_DELAY);
};

var serverLeaveCluster = function (socket, respond) {
  delete serverInstanceSockets[socket.instanceId];

  setClusterScaleTimeout(() => {
    sendEventToAllInstances(clientInstanceSockets, 'serverLeaveCluster', getServerClusterState());
  });

  respond && respond();
  logInfo(`Server ${socket.instanceId} at address ${socket.instanceIp} on port ${socket.instancePort} left the cluster`)
};

var clientLeaveCluster = function (socket, respond) {
  delete clientInstanceSockets[socket.instanceId];
  respond && respond();
  logInfo(`Client ${socket.instanceId} at address ${socket.instanceIp} left the cluster`);
};

var checkClientStatesConvergence = function (socketList) {
  var prevInstanceState = null;
  var allStatesEqual = true;
  _.forEach(socketList, function (socket) {
    if (prevInstanceState && prevInstanceState != socket.instanceState) {
      allStatesEqual = false;
      return;
    }
    prevInstanceState = socket.instanceState;
  });
  return allStatesEqual;
};

var sendEventToInstance = function (socket, event, data) {
  socket.emit(event, data, function (err) {
    if (err) {
      logError(err);
      if (socket.state == 'open') {
        setTimeout(sendEventToInstance.bind(null, socket, event, data), RETRY_DELAY);
      }
    }
  });
};

var sendEventToAllInstances = function (instances, event, data) {
  _.forEach(instances, function (socket) {
    sendEventToInstance(socket, event, data);
  });
};

var getRemoteIp = function (socket, data) {
  var forwardedAddress = FORWARDED_FOR_HEADER ? (socket.request.headers[FORWARDED_FOR_HEADER] || '').split(',')[0] : null;
  return data.instanceIp || forwardedAddress || socket.remoteAddress;
};

scServer.on('error', function (err) {
  logError(err);
});

scServer.on('warning', function (err) {
  logWarn(err);
});

if (AUTH_KEY) {
  scServer.addMiddleware(scServer.MIDDLEWARE_HANDSHAKE, (req, next) => {
    var urlParts = url.parse(req.url, true);
    if (urlParts.query && urlParts.query.authKey == AUTH_KEY) {
      next();
    } else {
      var err = new Error('Cannot connect to the cluster state server without providing a valid authKey as a URL query argument.');
      err.name = 'BadClusterAuthError';
      next(err);
    }
  });
}

scServer.on('connection', function (socket) {
  socket.on('error', (err) => {
    logError(err);
  });
  socket.on('serverJoinCluster', function (data, respond) {
    socket.instanceType = 'server';
    socket.instanceId = data.instanceId;
    socket.instanceIp = getRemoteIp(socket, data);
    if (data.instanceIp) {
      socket.instanceIpFamily = data.instanceIpFamily;
    }
    socket.instancePort = data.instancePort;
    socket.instanceSecure = data.instanceSecure;
    serverInstanceSockets[data.instanceId] = socket;

    setClusterScaleTimeout(() => {
      sendEventToAllInstances(clientInstanceSockets, 'serverJoinCluster', getServerClusterState());
    });

    respond();
    logInfo(`Server ${data.instanceId} at address ${socket.instanceIp} on port ${socket.instancePort} joined the cluster`);
  });
  socket.on('serverLeaveCluster', function (respond) {
    serverLeaveCluster(socket, respond);
  });
  socket.on('clientJoinCluster', function (data, respond) {
    socket.instanceType = 'client';
    socket.instanceId = data.instanceId;
    socket.instanceIp = getRemoteIp(socket, data);
    clientInstanceSockets[data.instanceId] = socket;
    respond(null, getServerClusterState());
    logInfo(`Client ${data.instanceId} at address ${socket.instanceIp} joined the cluster`);
  });
  socket.on('clientLeaveCluster', function (respond) {
    clientLeaveCluster(socket, respond);
  });
  socket.on('clientSetState', function (data, respond) {
    socket.instanceState = data.instanceState;
    var clientStatesConverge = checkClientStatesConvergence(clientInstanceSockets);
    if (clientStatesConverge) {
      sendEventToAllInstances(clientInstanceSockets, 'clientStatesConverge', {state: socket.instanceState});
      logInfo(`Cluster state converged to ${socket.instanceState}`);
    }
    respond();
  });
  socket.on('disconnect', function () {
    if (socket.instanceType == 'server') {
      serverLeaveCluster(socket);
    } else if (socket.instanceType == 'client') {
      clientLeaveCluster(socket);
    }
  });
});

httpServer.listen(PORT);
httpServer.on('listening', function () {
  logInfo(`SC Cluster State Server is listening on port ${PORT}`);
});

function logError(err) {
  if (LOG_LEVEL > 0) {
    console.error(err);
  }
}

function logWarn(warn) {
  if (LOG_LEVEL >= 2) {
    console.warn(warn);
  }
}

function logInfo(info) {
  if (LOG_LEVEL >= 3) {
    console.info(info);
  }
}
