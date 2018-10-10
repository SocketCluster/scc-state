var argv = require('minimist')(process.argv.slice(2));
var http = require('http');
var socketCluster = require('socketcluster-server');
var url = require('url');
var semverRegex = /\d+\.\d+\.\d+/;
var packageVersion = require(`./package.json`).version;
var requiredMajorSemver = getMajorSemver(packageVersion);

var DEFAULT_PORT = 7777;
var DEFAULT_CLUSTER_SCALE_OUT_DELAY = 5000;
var DEFAULT_CLUSTER_SCALE_BACK_DELAY = 1000;
var DEFAULT_CLUSTER_STARTUP_DELAY = 5000;

var PORT = Number(argv.p) || Number(process.env.SCC_STATE_SERVER_PORT) || DEFAULT_PORT;
var AUTH_KEY = process.env.SCC_AUTH_KEY || null;
var FORWARDED_FOR_HEADER = process.env.FORWARDED_FOR_HEADER || null;
var RETRY_DELAY = Number(argv.r) || Number(process.env.SCC_STATE_SERVER_RETRY_DELAY) || 2000;
var CLUSTER_SCALE_OUT_DELAY = selectNumericArgument([argv.d, process.env.SCC_STATE_SERVER_SCALE_OUT_DELAY, DEFAULT_CLUSTER_SCALE_OUT_DELAY]);
var CLUSTER_SCALE_BACK_DELAY = selectNumericArgument([argv.d, process.env.SCC_STATE_SERVER_SCALE_BACK_DELAY, DEFAULT_CLUSTER_SCALE_BACK_DELAY]);
var STARTUP_DELAY = selectNumericArgument([argv.s, process.env.SCC_STATE_SERVER_STARTUP_DELAY, DEFAULT_CLUSTER_STARTUP_DELAY]);

function selectNumericArgument(args) {
  var lastIndex = args.length - 1;
  for (var i = 0; i < lastIndex; i++) {
    var current = Number(args[i]);
    if (!isNaN(current) && args[i] != null) {
      return current;
    }
  }
  return Number(args[lastIndex]);
};

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
var serverReady = STARTUP_DELAY > 0 ? false : true;
if (!serverReady) {
  logInfo(`Waiting ${STARTUP_DELAY}ms for initial scc-broker instances before allowing scc-worker instances to join`);
  setTimeout(function() {
    logInfo('State server is now allowing scc-worker instances to join the cluster');
    serverReady = true;
  }, STARTUP_DELAY);
}

var getSCCBrokerClusterState = function () {
  var sccBrokerURILookup = {};
  Object.keys(sccBrokerSockets).forEach((socketId) => {
    var socket = sccBrokerSockets[socketId];
    var targetProtocol = socket.instanceSecure ? 'wss' : 'ws';
    var instanceIp;
    if (socket.instanceIpFamily === 'IPv4') {
      instanceIp = socket.instanceIp;
    } else {
      instanceIp = `[${socket.instanceIp}]`;
    }
    var instanceURI = `${targetProtocol}://${instanceIp}:${socket.instancePort}`;
    sccBrokerURILookup[instanceURI] = true;
  });
  return {
    sccBrokerURIs: Object.keys(sccBrokerURILookup),
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
  delete sccBrokerSockets[socket.id];
  setClusterScaleTimeout(() => {
    sendEventToAllInstances(sccWorkerSockets, 'sccBrokerLeaveCluster', getSCCBrokerClusterState());
  }, CLUSTER_SCALE_BACK_DELAY);

  respond && respond();
  logInfo(`The scc-broker instance ${socket.instanceId} at address ${socket.instanceIp} on port ${socket.instancePort} left the cluster on socket ${socket.id}`);
};

var sccWorkerLeaveCluster = function (socket, respond) {
  delete sccWorkerSockets[socket.id];
  respond && respond();
  logInfo(`The scc-worker instance ${socket.instanceId} at address ${socket.instanceIp} left the cluster on socket ${socket.id}`);
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
  Object.keys(instances).forEach((socketId) => {
    var socket = instances[socketId];
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

scServer.addMiddleware(scServer.MIDDLEWARE_HANDSHAKE_SC, (req, next) => {
  var remoteAddress = req.socket.remoteAddress;
  var urlParts = url.parse(req.socket.request.url, true);
  var { version, instanceType, instancePort } = urlParts.query;

  req.socket.instanceType = instanceType;
  req.socket.instancePort = instancePort;

  var reportedMajorSemver = getMajorSemver(version);
  var sccComponentIsObsolete = (!instanceType || Number.isNaN(reportedMajorSemver));
  var err;

  if (reportedMajorSemver === requiredMajorSemver) {
    return next();
  } else if (sccComponentIsObsolete) {
    err = new Error(`An obsolete SCC component at address ${remoteAddress} is incompatible with the scc-state@^${packageVersion}. Please, update the SCC component up to version ^${requiredMajorSemver}.0.0`);
  } else if (reportedMajorSemver > requiredMajorSemver) {
    err = new Error(`The scc-state@${packageVersion} is incompatible with the ${instanceType}@${version}. Please, update the scc-state up to version ^${reportedMajorSemver}.0.0`);
  } else {
    err = new Error(`The ${instanceType}@${version} at address ${remoteAddress}:${instancePort} is incompatible with the scc-state@^${packageVersion}. Please, update the ${instanceType} up to version ^${requiredMajorSemver}.0.0`);
  }

  err.name = 'CompatibilityError';
  next(err);
});

scServer.on('connection', function (socket) {
  socket.on('sccBrokerJoinCluster', function (data, respond) {
    socket.instanceId = data.instanceId;
    socket.instanceIp = getRemoteIp(socket, data);
    // Only set instanceIpFamily if data.instanceIp is provided.
    if (data.instanceIp) {
      socket.instanceIpFamily = data.instanceIpFamily;
    }
    socket.instanceSecure = data.instanceSecure;
    sccBrokerSockets[socket.id] = socket;

    setClusterScaleTimeout(() => {
      sendEventToAllInstances(sccWorkerSockets, 'sccBrokerJoinCluster', getSCCBrokerClusterState());
    }, CLUSTER_SCALE_OUT_DELAY);

    respond();
    logInfo(`The scc-broker instance ${data.instanceId} at address ${socket.instanceIp} on port ${socket.instancePort} joined the cluster on socket ${socket.id}`);
  });

  socket.on('sccBrokerLeaveCluster', function (respond) {
    sccBrokerLeaveCluster(socket, respond);
  });

  socket.on('sccWorkerJoinCluster', function (data, respond) {
    socket.instanceId = data.instanceId;
    socket.instanceIp = getRemoteIp(socket, data);
    // Only set instanceIpFamily if data.instanceIp is provided.
    if (data.instanceIp) {
      socket.instanceIpFamily = data.instanceIpFamily;
    }

    if (!serverReady) {
      logWarn(`The scc-worker instance ${data.instanceId} at address ${socket.instanceIp} on socket ${socket.id} was not allowed to join the cluster because the server is waiting for initial brokers`);
      return respond(new Error('The server is waiting for initial broker connections'));
    }

    sccWorkerSockets[socket.id] = socket;
    respond(null, getSCCBrokerClusterState());
    logInfo(`The scc-worker instance ${data.instanceId} at address ${socket.instanceIp} joined the cluster on socket ${socket.id}`);
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

function getMajorSemver(semver) {
  var semverIsValid = typeof semver === 'string' && semver.match(semverRegex);

  if (semverIsValid) {
    var majorSemver = semver.split('.')[0];
    return parseInt(majorSemver);
  } else {
    return NaN;
  }
}
