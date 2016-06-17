var _ = require('lodash');
var argv = require('minimist')(process.argv.slice(2));
var http = require('http');
var socketCluster = require('socketcluster-server');

var httpServer = http.createServer();
var scServer = socketCluster.attach(httpServer);

var serverInstances = {};
var clientInstances = {};

var checkClientStatesConvergence = function (instances) {
  var prevInstanceState = null;
  var allStatesEqual = true;
  _.forEach(instances, function (instanceData) {
    if (prevInstanceState && prevInstanceState != instanceData.state) {
      allStatesEqual = false;
      return;
    }
    prevInstanceState = instanceData.state;
  });
  return allStatesEqual;
};

// TODO: Whenever an instance does not acknowledge the receipt of the event, retry with exponential backoff
var sendEventToAllInstances = function (instances, event, data) {
  _.forEach(instances, function (instanceData) {
    instanceData.socket.emit(event, data);
  });
};

scServer.on('connection', function (socket) {
  socket.on('serverJoinCluster', function (data, respond) {
    serverInstances[data && data.instanceId] = {
      socket: socket
    };
    sendEventToAllInstances(clientInstances, 'clusterAddServer', data);
    respond();
  });
  socket.on('serverLeaveCluster', function (data, respond) {
    delete serverInstances[data && data.instanceId];
    respond();
  });
  socket.on('clientJoinCluster', function (data, respond) {
    clientInstances[data && data.instanceId] = {
      socket: socket
    };
    respond();
  });
  socket.on('clientLeaveCluster', function (data, respond) {
    delete clientInstances[data && data.instanceId];
    respond();
  });
  socket.on('clientSetState', function (data, respond) {
    clientInstances[data.instanceId].state = data.state;
    var clientStatesConverge = checkClientStatesConvergence(clientInstances);
    if (clientStatesConverge) {
      sendEventToAllInstances(clientInstances, 'clientStatesConverge', {state: data.state});
    }
    respond();
  });
});

httpServer.listen(8000);
