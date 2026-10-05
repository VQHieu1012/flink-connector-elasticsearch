# Apache Flink Elasticsearch Connector

This repository contains the official Apache Flink Elasticsearch connector.

## Apache Flink

Apache Flink is an open source stream processing framework with powerful stream- and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

## Building the Apache Flink Elasticsearch Connector from Source

Prerequisites:

* Unix-like environment (we use Linux, Mac OS X)
* Git
* Maven (we recommend version 3.8.6)
* Java 11

```
git clone https://github.com/apache/flink-connector-elasticsearch.git
cd flink-connector-elasticsearch
mvn clean package -DskipTests
```

The resulting jars can be found in the `target` directory of the respective module.

## Fusion Center Elasticsearch 8 SQL connector patches

Fusion Center builds `flink-sql-connector-elasticsearch8`, the shaded SQL uber
JAR used by Flink SQL Gateway. The runtime fixes are implemented in
`flink-connector-elasticsearch8`; Maven then includes that patched library in the
SQL JAR through the dependency and shade configuration of
`flink-sql-connector-elasticsearch8/pom.xml`.

### Patch responsibilities

#### `elasticsearch8_async_writer_failure_handling.patch`

This patch changes failure handling in `Elasticsearch8AsyncWriter`:

* logs the first failed bulk item with index, document ID, HTTP status, error type,
  and error reason;
* retries transient failures such as HTTP 408/429, selected 5xx responses,
  rejected execution, unavailable shards, timeouts, and connection failures;
* treats deterministic failures such as mapping conflicts, document parsing
  errors, authentication errors, and other non-retryable 4xx responses as fatal;
* prevents deterministic records from entering an infinite retry/split loop.

#### `elasticsearch8_nested_row_serialization.patch`

This patch changes `RowDataToMapConverter` and adds
`RowDataToMapConverterTest`:

* recursively converts nested Flink SQL `ROW`, `ARRAY`, and `MAP` values into
  JSON-native Java `Map` and `List` structures;
* preserves named fields such as Debezium geometry `wkb` and `srid`;
* prevents nested `Row` objects from being serialized as Flink bean metadata such
  as `{"kind":"INSERT","arity":2}`;
* tests a nullable geometry row and an array of geometry rows.

### Apply and format the patches

Run all commands from the repository root. Keep the patch files at the repository
root or adjust their paths accordingly.

```bash
git apply --check elasticsearch8_async_writer_failure_handling.patch
git apply --check elasticsearch8_nested_row_serialization.patch

git apply elasticsearch8_async_writer_failure_handling.patch
git apply elasticsearch8_nested_row_serialization.patch

mvn spotless:apply
git diff --check
git status --short
```

`mvn spotless:apply` is required after applying the patches. It formats the Java
sources according to the Flink project rules before Checkstyle and tests run.

### Run the focused test

```bash
mvn \
  -pl flink-connector-elasticsearch8 \
  -am \
  -DskipITs \
  -Dtest=RowDataToMapConverterTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

The test must pass before building the SQL connector. For a broader module test,
run:

```bash
mvn -pl flink-connector-elasticsearch8 -am -DskipITs test
```

### Build `flink-sql-connector-elasticsearch8`

```bash
mvn \
  -pl flink-sql-connector-elasticsearch8 \
  -am \
  clean package \
  -DskipTests
```

The final deployable artifact is:

```text
flink-sql-connector-elasticsearch8/target/
  flink-sql-connector-elasticsearch8-4.1-SNAPSHOT.jar
```

The JAR from `flink-connector-elasticsearch8/target` is the library JAR, not the
SQL Gateway artifact. Deploy the shaded JAR from the
`flink-sql-connector-elasticsearch8/target` directory.

### Verify the built JAR

```bash
JAR=flink-sql-connector-elasticsearch8/target/flink-sql-connector-elasticsearch8-4.1-SNAPSHOT.jar

test -f "$JAR"
jar tf "$JAR" | grep 'META-INF/services/org.apache.flink.table.factories.Factory'
jar tf "$JAR" | grep -E 'RowDataToMapConverter|Elasticsearch8AsyncWriter'
sha256sum "$JAR"
```

Publish the result under an immutable name, for example:

```text
flink-sql-connector-elasticsearch8-4.1-fc.1.jar
```

Do not place old and new Elasticsearch SQL connector JARs on the same Flink
classpath. A running Flink job does not load the new JAR automatically: cancel the
old job, deploy the new artifact to SQL Gateway and TaskManager-visible storage,
update the Airflow connector JAR URI, and then trigger the pipeline again.

## Developing Flink

The Flink committers use IntelliJ IDEA to develop the Flink codebase.
We recommend IntelliJ IDEA for developing projects that involve Scala code.

Minimal requirements for an IDE are:
* Support for Java and Scala (also mixed projects)
* Support for Maven with Java and Scala

### IntelliJ IDEA

The IntelliJ IDE supports Maven out of the box and offers a plugin for Scala development.

* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
* IntelliJ Scala Plugin: [https://plugins.jetbrains.com/plugin/?id=1347](https://plugins.jetbrains.com/plugin/?id=1347)

Check out our [Setting up IntelliJ](https://nightlies.apache.org/flink/flink-docs-master/flinkDev/ide_setup.html#intellij-idea) guide for details.

## Support

Don’t hesitate to ask!

Contact the developers and community on the [mailing lists](https://flink.apache.org/community.html#mailing-lists) if you need any help.

[Open an issue](https://issues.apache.org/jira/browse/FLINK) if you found a bug in Flink.

## Documentation

The documentation of Apache Flink is located on the website: [https://flink.apache.org](https://flink.apache.org)
or in the `docs/` directory of the source code.

## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it.
Contact us if you are looking for implementation tasks that fit your skills.
This article describes [how to contribute to Apache Flink](https://flink.apache.org/contributing/how-to-contribute.html).

## About

Apache Flink is an open source project of The Apache Software Foundation (ASF).
The Apache Flink project originated from the [Stratosphere](http://stratosphere.eu) research project.

