var _ = require('lodash');
var argv = require('minimist')(process.argv.slice(2));
var http = require('http');
var socketCluster = require('socketcluster-server');
var url = require('url');

var DEFAULT_PORT = 7777;
var DEFAULT_CLUSTER_SCALE_OUT_DELAY = 5000;
var DEFAULT_CLUSTER_SCALE_BACK_DELAY = 1000;

var RETRY_DELAY = Number(argv.r) || Number(process.env.SCC_STATE_SERVER_RETRY_DELAY) || 2000;
var PORT = Number(argv.p) || Number(process.env.SCC_STATE_SERVER_PORT) || DEFAULT_PORT;
var CLUSTER_SCALE_OUT_DELAY = Number(argv.d) || Number(process.env.SCC_STATE_SERVER_SCALE_OUT_DELAY) || DEFAULT_CLUSTER_SCALE_OUT_DELAY;
var CLUSTER_SCALE_BACK_DELAY = Number(argv.d) || Number(process.env.SCC_STATE_SERVER_SCALE_BACK_DELAY) || DEFAULT_CLUSTER_SCALE_BACK_DELAY;
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
  if (req.url === '/health-check') {
    res.writeHead(200, {'Content-Type': 'text/html'});
    res.end('OK');
  } else {
    res.writeHead(404, {'Content-Type': 'text/html'});
    res.end('Not found');
  }
});

var sccBrokerSockets = {};
var sccWorkerSockets = {};

var getSCCBrokerClusterState = function () {
  var sccBrokerURIs = [];
  _.forOwn(sccBrokerSockets, function (socket) {
    var targetProtocol = socket.instanceSecure ? 'wss' : 'ws';
    var instanceIp;
    if (socket.instanceIpFamily === 'IPv4') {
      instanceIp = socket.instanceIp;
    } else {
      instanceIp = `[${socket.instanceIp}]`;
    }
    var instanceURI = `${targetProtocol}://${instanceIp}:${socket.instancePort}`;
    sccBrokerURIs.push(instanceURI);
  });
  return {
    sccBrokerURIs: sccBrokerURIs,
    time: Date.now()
  };
};

var clusterResizeTimeout;

var setClusterScaleTimeout = function (callback, delay) {
  // Only the latest scale request counts.
  if (clusterResizeTimeout) {
    clearTimeout(clusterResizeTimeout);
  }
  clusterResizeTimeout = setTimeout(callback, delay);
};

var sccBrokerLeaveCluster = function (socket, respond) {
  delete sccBrokerSockets[socket.instanceId];
  setClusterScaleTimeout(() => {
    sendEventToAllInstances(sccWorkerSockets, 'sccBrokerLeaveCluster', getSCCBrokerClusterState());
  }, CLUSTER_SCALE_BACK_DELAY);

  respond && respond();
  logInfo(`The scc-broker instance ${socket.instanceId} at address ${socket.instanceIp} on port ${socket.instancePort} left the cluster`);
};

var sccWorkerLeaveCluster = function (socket, respond) {
  delete sccWorkerSockets[socket.instanceId];
  respond && respond();
  logInfo(`The scc-worker instance ${socket.instanceId} at address ${socket.instanceIp} left the cluster`);
};

var sendEventToInstance = function (socket, event, data) {
  socket.emit(event, data, function (err) {
    if (err) {
      logError(err);
      if (socket.state === 'open') {
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

var getRemoteIp = function(socket, data) {
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
  scServer.addMiddleware(scServer.MIDDLEWARE_HANDSHAKE_WS, (req, next) => {
    var urlParts = url.parse(req.url, true);
    if (urlParts.query && urlParts.query.authKey === AUTH_KEY) {
      next();
    } else {
      var err = new Error('Cannot connect to the scc-state instance without providing a valid authKey as a URL query argument.');
      err.name = 'BadClusterAuthError';
      next(err);
    }
  });
}

scServer.on('connection', function (socket) {
  socket.on('sccBrokerJoinCluster', function (data, respond) {
    socket.instanceType = 'scc-broker';
    socket.instanceId = data.instanceId;
    socket.instanceIp = getRemoteIp(socket, data);
    // Only set instanceIpFamily if data.instanceIp is provided.
    if (data.instanceIp) {
      socket.instanceIpFamily = data.instanceIpFamily;
    }
    socket.instancePort = data.instancePort;
    socket.instanceSecure = data.instanceSecure;
    sccBrokerSockets[data.instanceId] = socket;

    setClusterScaleTimeout(() => {
      sendEventToAllInstances(sccWorkerSockets, 'sccBrokerJoinCluster', getSCCBrokerClusterState());
    }, CLUSTER_SCALE_OUT_DELAY);

    respond();
    logInfo(`The scc-broker instance ${data.instanceId} at address ${socket.instanceIp} on port ${socket.instancePort} joined the cluster`);
  });

  socket.on('sccBrokerLeaveCluster', function (respond) {
    sccBrokerLeaveCluster(socket, respond);
  });

  socket.on('sccWorkerJoinCluster', function (data, respond) {
    socket.instanceType = 'scc-worker';
    socket.instanceId = data.instanceId;
    socket.instanceIp = getRemoteIp(socket, data);
    // Only set instanceIpFamily if data.instanceIp is provided.
    if (data.instanceIp) {
      socket.instanceIpFamily = data.instanceIpFamily;
    }
    sccWorkerSockets[data.instanceId] = socket;
    respond(null, getSCCBrokerClusterState());
    logInfo(`The scc-worker instance ${data.instanceId} at address ${socket.instanceIp} joined the cluster`);
  });

  socket.on('sccWorkerLeaveCluster', function (respond) {
    sccWorkerLeaveCluster(socket, respond);
  });

  socket.on('disconnect', function () {
    if (socket.instanceType === 'scc-broker') {
      sccBrokerLeaveCluster(socket);
    } else if (socket.instanceType === 'scc-worker') {
      sccWorkerLeaveCluster(socket);
    }
  });
});

httpServer.listen(PORT);
httpServer.on('listening', function () {
  logInfo(`The scc-state instance is listening on port ${PORT}`);
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
