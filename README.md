# agc-state
Cluster state tracking and notification engine for Asyngular clusters

### Usage

```js
AGC_STATE_LOG_LEVEL=0 node server.js
```

### Log levels

 * 3 - log everything
 * 2 - warnings and errors
 * 1 - errors only
 * 0 - log nothing

### Build and deploy to DockerHub

Replace `x.x.x` with the version number.

```
docker build -t socketcluster/agc-state:vx.x.x .
```

```
docker push socketcluster/agc-state:vx.x.x
```
