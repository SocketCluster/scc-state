# scc-state
Cluster state tracking and notification engine for SocketCluster clusters

### Log levels:
```js
node server.js -l 0
// or
SCC_STATE_LOG_LEVEL=0 node server.js
```
 * 3 - log everything
 * 2 - warnings and errors
 * 1 - errors only
 * 0 - log nothing

### Build and deploy to DockerHub

Replace `x.x.x` with the version number.

```
docker build -t socketcluster/scc-state:vx.x.x .
```

```
docker push socketcluster/scc-state:vx.x.x
```
