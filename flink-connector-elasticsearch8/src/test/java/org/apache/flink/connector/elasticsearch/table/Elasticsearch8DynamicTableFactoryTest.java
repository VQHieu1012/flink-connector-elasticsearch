/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.elasticsearch.sink.Operation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.connector.elasticsearch.table.TestContext.context;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Elasticsearch8DynamicTableFactory}. */
class Elasticsearch8DynamicTableFactoryTest {

    @Test
    void configuredMaxRecordSizeIsPropagatedToRuntimeSink() {
        Elasticsearch8DynamicTableFactory factory = new Elasticsearch8DynamicTableFactory();

        DynamicTableSink dynamicSink =
                factory.createDynamicTableSink(
                        context()
                                .withOption("connector", "elasticsearch-8")
                                .withOption("hosts", "http://localhost:9200")
                                .withOption("index", "test-index")
                                .withOption("sink.bulk-flush.max-size", "10mb")
                                .withOption("sink.max-record-size", "5mb")
                                .build());

        Sink<RowData> runtimeSink =
                ((SinkV2Provider) dynamicSink.getSinkRuntimeProvider(new MockContext()))
                        .createSink();

        assertThat(runtimeSink)
                .isInstanceOf(AsyncSinkBase.class)
                .extracting("maxRecordSizeInBytes")
                .isEqualTo(5L * 1024 * 1024);
    }

    @Test
    void maxRecordSizeCannotExceedBulkFlushMaxSize() {
        Elasticsearch8DynamicTableFactory factory = new Elasticsearch8DynamicTableFactory();

        assertThatThrownBy(
                        () ->
                                factory.createDynamicTableSink(
                                        context()
                                                .withOption("connector", "elasticsearch-8")
                                                .withOption("hosts", "http://localhost:9200")
                                                .withOption("index", "test-index")
                                                .withOption("sink.bulk-flush.max-size", "2mb")
                                                .withOption("sink.max-record-size", "5mb")
                                                .build()))
                .isInstanceOf(org.apache.flink.table.api.ValidationException.class)
                .hasMessage(
                        "'sink.max-record-size' must not exceed "
                                + "'sink.bulk-flush.max-size'. Got: 5242880 bytes and 2097152 bytes");
    }

    @Test
    void acceptsVersionConflictOptions() {
        Elasticsearch8DynamicTableFactory factory = new Elasticsearch8DynamicTableFactory();

        assertThat(
                        factory.createDynamicTableSink(
                                context()
                                        .withOption("connector", "elasticsearch-8")
                                        .withOption("hosts", "http://localhost:9200")
                                        .withOption("index", "test-index")
                                        .withOption("sink.retry-on-conflict", "3")
                                        .withOption("sink.version-conflict-policy", "ignore")
                                        .build()))
                .isInstanceOf(ElasticSearch8AsyncDynamicSink.class);
    }

    @Test
    void rejectsNegativeRetryOnConflict() {
        Elasticsearch8DynamicTableFactory factory = new Elasticsearch8DynamicTableFactory();

        assertThatThrownBy(
                        () ->
                                factory.createDynamicTableSink(
                                        context()
                                                .withOption("connector", "elasticsearch-8")
                                                .withOption("hosts", "http://localhost:9200")
                                                .withOption("index", "test-index")
                                                .withOption("sink.retry-on-conflict", "-1")
                                                .build()))
                .isInstanceOf(org.apache.flink.table.api.ValidationException.class)
                .hasMessageContaining("'sink.retry-on-conflict' must be non-negative");
    }

    @Test
    @SuppressWarnings("unchecked")
    void configuredRetryOnConflictReachesUpdateOperation() throws Exception {
        ResolvedSchema schema =
                new ResolvedSchema(
                        List.of(
                                Column.physical("id", DataTypes.STRING()),
                                Column.physical("value", DataTypes.STRING())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", List.of("id")));
        DynamicTableSink dynamicSink =
                new Elasticsearch8DynamicTableFactory()
                        .createDynamicTableSink(
                                context()
                                        .withSchema(schema)
                                        .withOption("connector", "elasticsearch-8")
                                        .withOption("hosts", "http://localhost:9200")
                                        .withOption("index", "test-index")
                                        .withOption("sink.retry-on-conflict", "3")
                                        .build());
        Sink<RowData> runtimeSink =
                ((SinkV2Provider) dynamicSink.getSinkRuntimeProvider(new MockContext()))
                        .createSink();
        Field converterField = AsyncSinkBase.class.getDeclaredField("elementConverter");
        converterField.setAccessible(true);
        ElementConverter<RowData, Operation> converter =
                (ElementConverter<RowData, Operation>) converterField.get(runtimeSink);
        GenericRowData update =
                GenericRowData.of(StringData.fromString("id-1"), StringData.fromString("updated"));
        update.setRowKind(RowKind.UPDATE_AFTER);

        UpdateOperation<?, ?> operation =
                (UpdateOperation<?, ?>) converter.apply(update, null).getBulkOperationVariant();

        assertThat(operation.retryOnConflict()).isEqualTo(3);
    }

    private static class MockContext implements DynamicTableSink.Context {
        @Override
        public boolean isBounded() {
            return false;
        }

        @Override
        public TypeInformation<?> createTypeInformation(DataType consumedDataType) {
            return null;
        }

        @Override
        public TypeInformation<?> createTypeInformation(LogicalType consumedLogicalType) {
            return null;
        }

        @Override
        public DynamicTableSink.DataStructureConverter createDataStructureConverter(
                DataType consumedDataType) {
            return null;
        }

        @Override
        public Optional<int[][]> getTargetColumns() {
            return Optional.empty();
        }
    }
}
