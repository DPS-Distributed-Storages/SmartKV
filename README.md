# SmartKV

## Prerequisites

- Java 11+
- Python 3.10+
- Lua 5.4+

----------------------------

## Build and run a DML node using Gradle

1. Set the `DML_HOSTNAME` environment variable to the hostname under which the node is accessible by clients. The default is `localhost`. 

2. Execute the `gradle run` command:
   ```
   ./gradlew -p node run
   ```

----------------------------

## Create a fat jar

Execute the `gradle shadowJar` command:

```
./gradlew shadowJar
```
