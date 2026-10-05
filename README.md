# Apache Flink Elasticsearch Connector

This repository is a fork of the [Apache Flink Elasticsearch Connector](https://github.com/apache/flink-connector-elasticsearch).
It retains the upstream project structure and includes additional changes to
the Elasticsearch 8 SQL connector. The upstream project remains the source for
general connector documentation and development.

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

## Connector changes

The following changes are implemented in `flink-connector-elasticsearch8`:

* **Async bulk failure handling** — `Elasticsearch8AsyncWriter` logs the failed
  document's index, ID, HTTP status, error type, and reason. It retries transient
  failures (including HTTP 408, 429, selected 5xx responses, and known timeout,
  transport, rejected-execution, or unavailable-shard errors) and fails the sink
  for non-retryable item failures instead of retrying them indefinitely.
* **Nested row serialization** — `RowDataToMapConverter` recursively converts
  SQL `ROW`, `ARRAY`, and `MAP` values into JSON-friendly maps and lists. This
  preserves named nested fields (for example, Debezium geometry `wkb` and
  `srid`) rather than serializing nested rows as Flink `Row` metadata.
* **Configurable record size** — the SQL sink option `sink.max-record-size`
  sets the maximum serialized size of one record; its default is 1 MiB. The
  value must be positive and no greater than `sink.bulk-flush.max-size` (which
  defaults to 2 MiB). Set both options when a record is larger than the default:

```sql
WITH (
  'sink.max-record-size' = '4mb',
  'sink.bulk-flush.max-size' = '4mb'
)
```

### SQL table options

Set these options in the connector table's `WITH` clause. `hosts` and `index`
are required; `—` means there is no connector-defined default.

| Option | Applies to | Default | Description |
| --- | --- | --- | --- |
| `hosts` | Source and sink | Required | One or more Elasticsearch HTTP(S) endpoints, including ports. |
| `index` | Source and sink | Required | Elasticsearch index to read from or write to. |
| `username` | Source and sink | — | Username for Elasticsearch authentication. Set together with `password`. |
| `password` | Source and sink | — | Password for Elasticsearch authentication. Set together with `username`. |
| `ssl.certificate-fingerprint` | Source and sink | — | SHA-256 fingerprint of the CA certificate used to verify HTTPS. |
| `connection.path-prefix` | Source and sink | — | Prefix added to Elasticsearch REST request paths. |
| `connection.request-timeout` | Source and sink | — | Maximum time to obtain a connection from the connection manager. |
| `connection.timeout` | Source and sink | — | Maximum time to establish a connection. |
| `socket.timeout` | Source and sink | — | Maximum inactivity between receiving consecutive response packets. |
| `format` | Source and sink | `json` | Data format used to decode input or encode output. The format must produce valid JSON documents. |
| `sink.parallelism` | Sink | — | Parallelism for the sink operator; by default Flink determines it from the execution plan. |
| `sink.delivery-guarantee` | Sink | `AT_LEAST_ONCE` | Delivery guarantee requested for sink writes. |
| `document-id.key-delimiter` | Sink | `_` | Delimiter used when composing a document ID from multiple key fields. |
| `sink.bulk-flush.max-actions` | Sink | `1000` | Maximum number of actions in a bulk request. |
| `sink.bulk-flush.max-buffered-actions` | Sink | `10000` | Maximum number of actions buffered by the sink. |
| `sink.bulk-flush.max-in-flight-actions` | Sink | `50` | Maximum number of uncompleted actions before writes are back-pressured. |
| `sink.bulk-flush.max-size` | Sink | `2mb` | Maximum size of buffered actions per bulk request. Must be at least 1 MiB and specified in whole-MiB increments. |
| `sink.bulk-flush.interval` | Sink | `1s` | Maximum interval before buffered actions are flushed. |
| `sink.max-record-size` | Sink | `1mb` | Maximum serialized size of one record. Must be positive and no greater than `sink.bulk-flush.max-size`. |
| `sink.retry-on-conflict` | Sink | `0` | Number of retries performed by Elasticsearch for an update that hits a version conflict. Applies to update operations only; must be non-negative. |
| `sink.version-conflict-policy` | Sink | `fail` | What the writer does if an update still returns a 409 `version_conflict_engine_exception`: `fail` fails the Flink task; `ignore` logs and counts the conflict, then drops that update. |
| `max-retries` (`lookup.max-retries` alias) | Vector search | `3` | Maximum retry attempts for a failed vector search request. |
| `vector-search.num-candidates` | Vector search | `100` | Number of candidate neighbors considered per shard during vector search. |
| `lookup.cache` | Lookup cache | `NONE` | Flink lookup-cache mode. Lookup caching is not active for this connector, whose source exposes vector search rather than lookup joins. |
| `lookup.partial-cache.max-rows` | Lookup cache | None | Maximum rows retained by Flink's partial lookup cache; not active for this connector. |
| `lookup.partial-cache.expire-after-access` | Lookup cache | None | Evict a cached row after this duration without access; not active for this connector. |
| `lookup.partial-cache.expire-after-write` | Lookup cache | None | Evict a cached row after this duration since it was written; not active for this connector. |
| `lookup.partial-cache.cache-missing-key` | Lookup cache | `true` | Whether Flink caches empty lookup results; not active for this connector. |

The `lookup.*` entries are Flink's standard lookup-cache options recognized by
the factory, but they do not enable lookup joins or caching in this connector.
See the [Flink 2.2 lookup-cache documentation](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/table/jdbc/#lookup-cache)
for the generic cache behavior.

Tests for these changes are in `flink-connector-elasticsearch8`, including
`Elasticsearch8AsyncWriterTest`, `RowDataToMapConverterTest`, and
`Elasticsearch8DynamicTableFactoryTest`. Run the module tests with:

```bash
mvn -pl flink-connector-elasticsearch8 -am -DskipITs test
```

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

