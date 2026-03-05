
# Submit Pipeline as JAR

This guide explains how to package your Flink CDC Pipeline definition (YAML) into a JAR file and submit it using the standard `flink run` command.

## Overview

Flink CDC provides two submission methods:

### Method 1: Shell Script (Default)
```bash
bash bin/flink-cdc.sh pipeline.yaml
```

### Method 2: JAR Submission (This Guide)
```bash
flink run -c org.apache.flink.cdc.cli.CliFrontend my-pipeline.jar
```

**Use JAR submission when:**
- You need to integrate with existing CI/CD pipelines
- You want to version control the entire pipeline (YAML + dependencies)
- You need to include custom UDF JARs
- You prefer the standard Flink job submission workflow

---

## Quick Start

### Step 1: Create Maven Project

Create a new Maven project with the following structure:

```
my-cdc-pipeline/
├── pom.xml
```

### Step 2: Refer to modifying the source and sink pipeline connectors‘s pom of pom.xml

```
<!-- Source Connectors (add what you need) -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-cdc-pipeline-connector-mysql</artifactId>
    <version>${flink.cdc.version}</version>
</dependency>

<!-- Sink Connectors (add what you need) -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-cdc-pipeline-connector-starrocks</artifactId>
    <version>${flink.cdc.version}</version>
</dependency>

```

### Step 3: Create Pipeline Definition

Create `pipeline.yaml` in the same directory as the program:

```yaml
source:
  type: mysql
  hostname: localhost
  port: 3306
  username: root
  password: password
  tables: mydb\..*

sink:
  type: doris
  fenodes: 127.0.0.1:8030
  username: root
  password: ""

pipeline:
  name: MySQL to Doris Pipeline
  parallelism: 2
```

### Step 4: Build the JAR

```bash
mvn clean package
```

This creates `target/my-cdc-pipeline-1.0-SNAPSHOT.jar`.

Application dir files like 
```

├── my-cdc-pipeline-1.0-SNAPSHOT.jar
├── pipeline.yaml

```

### Step 5: Submit to Flink

```bash1
# Submit to local cluster


# Submit to remote cluster
flink run \
  -m <jobmanager-host>:8081 \
  -c org.apache.flink.cdc.cli.CliFrontend \
  my-cdc-pipeline-1.0-SNAPSHOT.jar \
  pipeline.yaml

# Submit to YARN
flink run \
  -m yarn-cluster \
  -ynm "My CDC Pipeline" \
  -c org.apache.flink.cdc.cli.CliFrontend \
  my-cdc-pipeline-1.0-SNAPSHOT.jar \
  pipeline.yaml
```


---

## Advanced Usage

### Including Custom UDFs

If your pipeline uses custom UDFs, add them to your project:

**Project Structure:**
```
my-cdc-pipeline/
├── pom.xml
├── src/main/java/
│   └── com/example/udf/
│       └── MyCustomFunction.java
```

**MyCustomFunction.java:**
```java
package com.example.udf;

import org.apache.flink.cdc.common.udf.UserDefinedFunction;

public class MyCustomFunction implements UserDefinedFunction {
    public String eval(String input) {
        return input.toUpperCase();
    }
}
```

**pipeline.yaml with UDF:**
```yaml
source:
  type: mysql
  hostname: localhost
  port: 3306
  username: root
  password: password
  tables: mydb\..*

sink:
  type: doris
  fenodes: 127.0.0.1:8030
  username: root
  password: ""

transform:
  - source-table: mydb.orders
    projection: order_id, MY_UPPER(customer_name) as customer_name, amount
    filter: amount > 100

pipeline:
  name: MySQL to Doris with UDF
  parallelism: 2
  user-defined-function:
    - name: MY_UPPER
      classpath: com.example.udf.MyCustomFunction
```

Build and submit as before. The UDF class will be included in the JAR.

---

### Multiple Pipeline Definitions

You can package multiple YAML files and choose which one to run:

**Project Structure:**
```
my-cdc-pipeline/
├── pom.xml

The same dir with java applition 
├── pipeline-dev.yaml
├── pipeline-staging.yaml
└── pipeline-prod.yaml
```

**Submit specific pipeline:**
```bash
# Development
flink run -c org.apache.flink.cdc.cli.CliFrontend \
  my-cdc-pipeline-1.0-SNAPSHOT.jar \
  pipeline-dev.yaml

# Production
flink run -c org.apache.flink.cdc.cli.CliFrontend \
  my-cdc-pipeline-1.0-SNAPSHOT.jar \
  pipeline-prod.yaml
```

---

### Using External YAML File

If you prefer to keep YAML outside the JAR:

```bash
flink run -c org.apache.flink.cdc.cli.CliFrontend \
  my-cdc-pipeline-1.0-SNAPSHOT.jar \
  /path/to/external/pipeline.yaml
```

The `CliFrontend` will first look for the file in the JAR's resources, then fall back to the filesystem path.

---

### Passing Flink Configuration

You can override Flink configuration at submission time:

```bash
flink run \
  -c org.apache.flink.cdc.cli.CliFrontend \
  -D execution.checkpointing.interval=60s \
  -D state.backend=rocksdb \
  my-cdc-pipeline-1.0-SNAPSHOT.jar \
  pipeline.yaml
```

Or use the `--flink-conf` option:

```bash
flink run \
  -c org.apache.flink.cdc.cli.CliFrontend \
  my-cdc-pipeline-1.0-SNAPSHOT.jar \
  pipeline.yaml \
  --flink-conf execution.checkpointing.interval=60s \
  --flink-conf state.backend=rocksdb
```

---

### Savepoint and Resume

Resume from a savepoint:

```bash
flink run \
  -s hdfs:///flink/savepoints/savepoint-123456 \
  -c org.apache.flink.cdc.cli.CliFrontend \
  my-cdc-pipeline-1.0-SNAPSHOT.jar \
  pipeline.yaml
```

Or use the `--savepoint-path` option:

```bash
flink run \
  -c org.apache.flink.cdc.cli.CliFrontend \
  my-cdc-pipeline-1.0-SNAPSHOT.jar \
  pipeline.yaml \
  --savepoint-path hdfs:///flink/savepoints/savepoint-123456
```

---

## Connector Dependencies

Add only the connectors you need to reduce JAR size:



## Troubleshooting

### Issue: ClassNotFoundException

**Cause**: Missing connector dependency in pom.xml

**Solution**: Add the required connector dependency and rebuild:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-cdc-pipeline-connector-mysql</artifactId>
    <version>${flink.cdc.version}</version>
</dependency>
```

### Issue: "Pipeline definition file not found"

**Cause**: YAML file not in JAR resources or wrong path

**Solution**: Ensure YAML is in `classpath` and use the correct filename:

```bash
# Correct (file in JAR resources)
flink run -c org.apache.flink.cdc.cli.CliFrontend app.jar pipeline.yaml

# Correct (absolute path)
flink run -c org.apache.flink.cdc.cli.CliFrontend app.jar /yaml/pipeline.yaml
```

### Issue: JAR size too large

**Solution**: Exclude unnecessary dependencies:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-cdc-pipeline-connector-mysql</artifactId>
    <version>${flink.cdc.version}</version>
    <exclusions>
        <exclusion>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-shaded-hadoop-2-uber</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

### Issue: "FLINK_HOME not set"

**Cause**: Some deployment modes require FLINK_HOME

**Solution**: Set environment variable or use `--flink-home` option:

```bash
export FLINK_HOME=/path/to/flink

# Or
flink run -c org.apache.flink.cdc.cli.CliFrontend \
  app.jar pipeline.yaml \
  --flink-home /path/to/flink
```

---


**Build and Submit:**
```bash
# Build
mvn clean package

# Submit to Flink cluster
flink run \
  -m localhost:8081 \
  -c org.apache.flink.cdc.cli.CliFrontend \
  mysql-to-kafka-cdc-1.0.0.jar \
  pipeline.yaml
```

---

## Summary

JAR submission provides a standard, CI/CD-friendly way to deploy Flink CDC pipelines:

1. **Create Maven project** with pipeline YAML in the same with application dir
2. **Add connector dependencies** for your source and sink
3. **Configure Maven Shade plugin** with `CliFrontend` as main class
4. **Build JAR** with `mvn clean package`
5. **Submit** with `flink run -c org.apache.flink.cdc.cli.CliFrontend app.jar pipeline.yaml`

This approach gives you full control over dependencies, versioning, and deployment while maintaining compatibility with standard Flink tooling.
