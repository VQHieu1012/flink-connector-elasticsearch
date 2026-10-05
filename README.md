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

## Fusion Center Elasticsearch 8 SQL connector customizations

The `flink-connector-elasticsearch8` module includes the following runtime
customizations used by Fusion Center:

* Async bulk failures include details about the failed item, retry transient
  failures, and fail fast on deterministic document or configuration errors.
* Nested SQL `ROW`, `ARRAY`, and `MAP` values are converted to JSON-native maps
  and lists, preserving named fields such as Debezium geometry `wkb` and `srid`.
* The SQL sink option `sink.max-record-size` controls the maximum serialized
  Elasticsearch document size. Its default is 1 MiB. It must not exceed
  `sink.bulk-flush.max-size`, so increase both options for larger documents:

```sql
WITH (
  'sink.max-record-size' = '4mb',
  'sink.bulk-flush.max-size' = '4mb'
)
```

Focused tests are available in `flink-connector-elasticsearch8`, including
`RowDataToMapConverterTest` and `Elasticsearch8DynamicTableFactoryTest`.
To build the deployable shaded SQL connector JAR, run:

```bash
mvn -pl flink-sql-connector-elasticsearch8 -am clean package -DskipTests
```

Deploy the shaded JAR from `flink-sql-connector-elasticsearch8/target` to the
SQL Gateway and TaskManager-visible storage; a running Flink job must be restarted
to load the new connector.

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

