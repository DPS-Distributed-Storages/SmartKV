# Java client

## Prerequisites

- Java 11+
- Lua 5.4+

----------------------------

## Installation

Install the client to the local Maven repository by running the following command in the root directory of the DML project:

```bash
./gradlew publishToMavenLocal
```

## Usage

### Gradle

In your `build.gradle`, add Maven local to the repositories:

```
repositories {
    ...
    mavenLocal()
    ...
}
```

Add the client to the dependencies:

```
implementation "Apollo-Tools-DML:client:1.0-SNAPSHOT"
```
