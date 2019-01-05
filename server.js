var argv = require('minimist')(process.argv.slice(2));
var http = require('http');
var asyngularServer = require('asyngular-server');
var url = require('url');
var semverRegex = /\d+\.\d+\.\d+/;
var eetase = require('eetase');
var packageVersion = require(`./package.json`).version;
var requiredMajorSemver = getMajorSemver(packageVersion);

var DEFAULT_PORT = 7777;
var DEFAULT_CLUSTER_SCALE_OUT_DELAY = 5000;
var DEFAULT_CLUSTER_SCALE_BACK_DELAY = 1000;
var DEFAULT_CLUSTER_STARTUP_DELAY = 5000;

var PORT = Number(argv.p) || Number(process.env.AGC_STATE_SERVER_PORT) || DEFAULT_PORT;
var AUTH_KEY = process.env.AGC_AUTH_KEY || null;
var FORWARDED_FOR_HEADER = process.env.FORWARDED_FOR_HEADER || null;
var RETRY_DELAY = Number(argv.r) || Number(process.env.AGC_STATE_SERVER_RETRY_DELAY) || 2000;
var CLUSTER_SCALE_OUT_DELAY = selectNumericArgument([argv.d, process.env.AGC_STATE_SERVER_SCALE_OUT_DELAY, DEFAULT_CLUSTER_SCALE_OUT_DELAY]);
var CLUSTER_SCALE_BACK_DELAY = selectNumericArgument([argv.d, process.env.AGC_STATE_SERVER_SCALE_BACK_DELAY, DEFAULT_CLUSTER_SCALE_BACK_DELAY]);
var STARTUP_DELAY = selectNumericArgument([argv.s, process.env.AGC_STATE_SERVER_STARTUP_DELAY, DEFAULT_CLUSTER_STARTUP_DELAY]);

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
} else if (typeof process.env.AGC_STATE_LOG_LEVEL !== 'undefined') {
  LOG_LEVEL = Number(process.env.AGC_STATE_LOG_LEVEL);
} else {
  LOG_LEVEL = 3;
}

var httpServer = eetase(http.createServer());
var agServer = asyngularServer.attach(httpServer);

(async () => {
  for await (let [req, res] of httpServer.listener('request')) {
    if (req.url === '/health-check') {
      res.writeHead(200, {'Content-Type': 'text/html'});
      res.end('OK');
    } else {
      res.writeHead(404, {'Content-Type': 'text/html'});
      res.end('Not found');
    }
  }
})();

var agcBrokerSockets = {};
var agcWorkerSockets = {};
var serverReady = STARTUP_DELAY > 0 ? false : true;
if (!serverReady) {
  logInfo(`Waiting ${STARTUP_DELAY}ms for initial agc-broker instances before allowing agc-worker instances to join`);
  setTimeout(function() {
    logInfo('State server is now allowing agc-worker instances to join the cluster');
    serverReady = true;
  }, STARTUP_DELAY);
}

var getAGCBrokerClusterState = function () {
  var agcBrokerURILookup = {};
  Object.keys(agcBrokerSockets).forEach((socketId) => {
    var socket = agcBrokerSockets[socketId];
    var targetProtocol = socket.instanceSecure ? 'wss' : 'ws';
    var instanceIp;
    if (socket.instanceIpFamily === 'IPv4') {
      instanceIp = socket.instanceIp;
    } else {
      instanceIp = `[${socket.instanceIp}]`;
    }
    var instanceURI = `${targetProtocol}://${instanceIp}:${socket.instancePort}`;
    agcBrokerURILookup[instanceURI] = true;
  });
  return {
    agcBrokerURIs: Object.keys(agcBrokerURILookup),
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

var agcBrokerLeaveCluster = function (socket, req) {
  delete agcBrokerSockets[socket.id];
  setClusterScaleTimeout(() => {
    invokeRPCOnAllInstances(agcWorkerSockets, 'agcBrokerLeaveCluster', getAGCBrokerClusterState());
  }, CLUSTER_SCALE_BACK_DELAY);

  if (req) {
    req.end();
  }
  logInfo(`The agc-broker instance ${socket.instanceId} at address ${socket.instanceIp} on port ${socket.instancePort} left the cluster on socket ${socket.id}`);
};

var agcWorkerLeaveCluster = function (socket, req) {
  delete agcWorkerSockets[socket.id];
  if (req) {
    req.end();
  }
  logInfo(`The agc-worker instance ${socket.instanceId} at address ${socket.instanceIp} left the cluster on socket ${socket.id}`);
};

var invokeRPCOnInstance = async function (socket, procedureName, data) {
  try {
    await socket.invoke(procedureName);
  } catch (err) {
    logError(err);
    if (socket.state === 'open') {
      setTimeout(invokeRPCOnInstance.bind(null, socket, procedureName, data), RETRY_DELAY);
    }
  }
};

var invokeRPCOnAllInstances = function (instances, event, data) {
  Object.keys(instances).forEach((socketId) => {
    var socket = instances[socketId];
    invokeRPCOnInstance(socket, event, data);
  });
};

var getRemoteIp = function (socket, data) {
  var forwardedAddress = FORWARDED_FOR_HEADER ? (socket.request.headers[FORWARDED_FOR_HEADER] || '').split(',')[0] : null;
  return data.instanceIp || forwardedAddress || socket.remoteAddress;
};

(async () => {
  for await (let {error} of agServer.listener('error')) {
    logError(error);
  }
})();

(async () => {
  for await (let {warning} of agServer.listener('warning')) {
    logWarning(warning);
  }
})();

if (AUTH_KEY) {
  agServer.addMiddleware(agServer.MIDDLEWARE_HANDSHAKE_WS, (req, next) => {
    var urlParts = url.parse(req.url, true);
    if (urlParts.query && urlParts.query.authKey === AUTH_KEY) {
      next();
    } else {
      var err = new Error('Cannot connect to the agc-state instance without providing a valid authKey as a URL query argument.');
      err.name = 'BadClusterAuthError';
      next(err);
    }
  });
}

agServer.addMiddleware(agServer.MIDDLEWARE_HANDSHAKE_SC, (req, next) => {
  var remoteAddress = req.socket.remoteAddress;
  var urlParts = url.parse(req.socket.request.url, true);
  var { version, instanceType, instancePort } = urlParts.query;

  req.socket.instanceType = instanceType;
  req.socket.instancePort = instancePort;

  var reportedMajorSemver = getMajorSemver(version);
  var agcComponentIsObsolete = (!instanceType || Number.isNaN(reportedMajorSemver));
  var err;

  if (reportedMajorSemver === requiredMajorSemver) {
    return next();
  } else if (agcComponentIsObsolete) {
    err = new Error(`An obsolete AGC component at address ${remoteAddress} is incompatible with the agc-state@^${packageVersion}. Please, update the AGC component up to version ^${requiredMajorSemver}.0.0`);
  } else if (reportedMajorSemver > requiredMajorSemver) {
    err = new Error(`The agc-state@${packageVersion} is incompatible with the ${instanceType}@${version}. Please, update the agc-state up to version ^${reportedMajorSemver}.0.0`);
  } else {
    err = new Error(`The ${instanceType}@${version} at address ${remoteAddress}:${instancePort} is incompatible with the agc-state@^${packageVersion}. Please, update the ${instanceType} up to version ^${requiredMajorSemver}.0.0`);
  }

  err.name = 'CompatibilityError';
  next(err);
});

(async () => {
  for await (let {socket} of agServer.listener('connection')) {

    (async () => {
      for await (let req of socket.procedure('agcBrokerJoinCluster')) {
        let data = req.data;
        socket.instanceId = data.instanceId;
        socket.instanceIp = getRemoteIp(socket, data);
        // Only set instanceIpFamily if data.instanceIp is provided.
        if (data.instanceIp) {
          socket.instanceIpFamily = data.instanceIpFamily;
        }
        socket.instanceSecure = data.instanceSecure;
        agcBrokerSockets[socket.id] = socket;

        setClusterScaleTimeout(() => {
          invokeRPCOnAllInstances(agcWorkerSockets, 'agcBrokerJoinCluster', getAGCBrokerClusterState());
        }, CLUSTER_SCALE_OUT_DELAY);

        req.end();
        logInfo(`The agc-broker instance ${data.instanceId} at address ${socket.instanceIp} on port ${socket.instancePort} joined the cluster on socket ${socket.id}`);
      }
    })();

    (async () => {
      for await (let req of socket.procedure('agcBrokerLeaveCluster')) {
        agcBrokerLeaveCluster(socket, req);
      }
    })();

    (async () => {
      for await (let req of socket.procedure('agcWorkerJoinCluster')) {
        let data = req.data;
        socket.instanceId = data.instanceId;
        socket.instanceIp = getRemoteIp(socket, data);
        // Only set instanceIpFamily if data.instanceIp is provided.
        if (data.instanceIp) {
          socket.instanceIpFamily = data.instanceIpFamily;
        }

        if (!serverReady) {
          logWarning(`The agc-worker instance ${data.instanceId} at address ${socket.instanceIp} on socket ${socket.id} was not allowed to join the cluster because the server is waiting for initial brokers`);
          req.error(
            new Error('The server is waiting for initial broker connections')
          );
          continue;
        }

        agcWorkerSockets[socket.id] = socket;
        req.end(getAGCBrokerClusterState());
        logInfo(`The agc-worker instance ${data.instanceId} at address ${socket.instanceIp} joined the cluster on socket ${socket.id}`);
      }
    })();

    (async () => {
      for await (let req of socket.procedure('agcWorkerLeaveCluster')) {
        agcWorkerLeaveCluster(socket, req);
      }
    })();

    (async () => {
      for await (let event of socket.listener('disconnect')) {
        if (socket.instanceType === 'agc-broker') {
          agcBrokerLeaveCluster(socket);
        } else if (socket.instanceType === 'agc-worker') {
          agcWorkerLeaveCluster(socket);
        }
      }
    })();

  }
})();

(async () => {
  for await (let event of httpServer.listener('listening')) {
    logInfo(`The agc-state instance is listening on port ${PORT}`);
  }
})();

httpServer.listen(PORT);

function logError(err) {
  if (LOG_LEVEL > 0) {
    console.error(err);
  }
}

function logWarning(warn) {
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
