
# Usage

## Build and Execute
1. In the head directory of this repository execute
```bash
./gradlew -p node build -x test
```
to build the dml node's source code. 

2. Start a storage node via executing:
```bash
VERTX_CONFIG_PATH=conf/config.json java -Dvertx.jgroups.config=conf/jgroups-tcp.xml -jar node/build/libs/node-1.0-SNAPSHOT-all.jar
```
Note that `VERTX_CONFIG_PATH=<path_to_your_node_configuration>` specifies the json configuration file of your storage node. It includes all information required by a storage node such as its hostname, its zone and region, its unit prices, etc.
By default, the `conf/config.json` file will be loaded. Some other sample configuration files are provided specifying different regions and zones.

Moreover, the jgroups configuration can be specified via `-Dvertx.jgroups.config=<path_to_your_jgroups_configuration_file>`. [JGroups](http://www.jgroups.org/) is utilized for node discovery among a cluster of distributed storage nodes. If no configuration file is provided, JGroups will use a default configuration which may require multicast network traffic. Depending on your setup and networking configurations you may have to adapt the default configuration of JGroups. Please check https://vertx.io/docs/vertx-infinispan/java/ and http://www.jgroups.org/ for further details.

Alternatively you can directly run a storage node locally with all default configurations via:
```bash
./gradlew -p node run
```

## Docker

The node project has dependencies to other files in the repository. Therefore, make sure to set the build context to the root of the repository.

If you are in this directory, run the following command to build the docker image:
```bash
docker build -t dml:latest ../
```

Or, if you are in the root of the repository, run the following command:
```bash
docker build -t dml:latest . -f node/Dockerfile
```

If you are running the Dockerfile from within IntelliJ, you can set `Context folder` to the root of the repository in the Run Configuration dialog.
