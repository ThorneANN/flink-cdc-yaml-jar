
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
flink run -c EmbeddedPipelineMain my-pipeline.jar pipeline.yaml
```

**Use JAR submission when:**
- You need to integrate with existing CI/CD pipelines
- You want to version control the entire pipeline (YAML + dependencies) in one place
- You need to include custom UDF JARs
- You prefer the standard Flink job submission workflow

---

## Quick Start

### Step 1: Project Structure

```
my-cdc-pipeline/
├── pom.xml
└── src/main/
    ├── java/
    │   └── EmbeddedPipelineMain.java   ← wrapper entry point
    └── resources/
        ├── pipeline-dev.yaml           ← packaged into JAR
        └── pipeline-prod.yaml
```

### Step 2: Configure Connectors in pom.xml

```xml
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

Place your YAML files under `src/main/resources/`. They will be packaged into the JAR automatically.

```yaml
source:
  type: mysql
  hostname: localhost
  port: 3306
  username: root
  password: password
  tables: mydb\..*

sink:
  type: starrocks
  jdbc-url: jdbc:mysql://127.0.0.1:9030
  load-url: 127.0.0.1:8080
  username: root
  password: ""

pipeline:
  name: MySQL to StarRocks Pipeline
  parallelism: 2
```

### Step 4: Build the JAR

```bash
mvn clean package
```

This creates `target/my-cdc-pipeline-1.0-SNAPSHOT.jar` with all YAML files embedded inside.

### Step 5: Submit to Flink

```bash
# Load pipeline-dev.yaml from inside the JAR
flink run -c EmbeddedPipelineMain \
  target/my-cdc-pipeline-1.0-SNAPSHOT.jar \
  pipeline-dev.yaml

# Load pipeline-prod.yaml from inside the JAR
flink run -c EmbeddedPipelineMain \
  target/my-cdc-pipeline-1.0-SNAPSHOT.jar \
  pipeline-prod.yaml
```

The `EmbeddedPipelineMain` wrapper:
1. Looks for the given filename inside the JAR (classpath)
2. If found, extracts it to a temp file and passes it to `CliFrontend`
3. If not found in the JAR, falls back to treating it as a filesystem path

---

## Advanced Usage

### Using an External YAML File

If you prefer to use a YAML file outside the JAR, just pass an absolute path — the fallback will handle it automatically:

```bash
flink run -c EmbeddedPipelineMain \
  my-cdc-pipeline-1.0-SNAPSHOT.jar \
  /path/to/external/pipeline.yaml
```

---

### Submit to Remote Cluster

```bash
flink run \
  -m <jobmanager-host>:8081 \
  -c EmbeddedPipelineMain \
  my-cdc-pipeline-1.0-SNAPSHOT.jar \
  pipeline-prod.yaml
```

### Submit to YARN

```bash
flink run \
  -m yarn-cluster \
  -ynm "My CDC Pipeline" \
  -c EmbeddedPipelineMain \
  my-cdc-pipeline-1.0-SNAPSHOT.jar \
  pipeline-prod.yaml
```

---

### Passing Flink Configuration

```bash
flink run \
  -c EmbeddedPipelineMain \
  my-cdc-pipeline-1.0-SNAPSHOT.jar \
  pipeline.yaml \
  --flink-conf execution.checkpointing.interval=60s \
  --flink-conf state.backend=rocksdb
```

---

### Savepoint and Resume

```bash
flink run \
  -s hdfs:///flink/savepoints/savepoint-123456 \
  -c EmbeddedPipelineMain \
  my-cdc-pipeline-1.0-SNAPSHOT.jar \
  pipeline.yaml \
  --savepoint-path hdfs:///flink/savepoints/savepoint-123456
```

---

### Including Custom UDFs

```
my-cdc-pipeline/
├── pom.xml
└── src/main/
    ├── java/
    │   ├── EmbeddedPipelineMain.java
    │   └── com/example/udf/
    │       └── MyCustomFunction.java
    └── resources/
        └── pipeline.yaml
```

```yaml
transform:
  - source-table: mydb.orders
    projection: order_id, MY_UPPER(customer_name) as customer_name, amount

pipeline:
  name: MySQL to StarRocks with UDF
  parallelism: 2
  user-defined-function:
    - name: MY_UPPER
      classpath: com.example.udf.MyCustomFunction
```

---

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

**Cause**: YAML filename doesn't match what's in `src/main/resources/`

**Solution**: Check the exact filename and rebuild:

```bash
# List embedded YAML files in the JAR
jar tf my-cdc-pipeline-1.0-SNAPSHOT.jar | grep .yaml
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

**Solution**: Set environment variable or use `--flink-home` option:

```bash
export FLINK_HOME=/path/to/flink

# Or
flink run -c EmbeddedPipelineMain \
  app.jar pipeline.yaml \
  --flink-home /path/to/flink
```

---

## Summary

1. Place YAML files in `src/main/resources/` — they are packaged into the JAR
2. Add connector dependencies for your source and sink
3. Use `EmbeddedPipelineMain` as the main class (wraps `CliFrontend`)
4. Build with `mvn clean package`
5. Submit with `flink run -c EmbeddedPipelineMain app.jar <yaml-filename>`

The YAML filename is passed as an argument at runtime, so you can switch between environments without rebuilding the JAR.