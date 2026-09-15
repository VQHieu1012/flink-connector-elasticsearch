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

package org.apache.flink.connector.elasticsearch.sink;

import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;

class Elasticsearch8AsyncWriterTest {

    @Test
    void classifiesOnlyTransientBulkFailuresAsRetryable() {
        assertThat(
                        Elasticsearch8AsyncWriter.isTransientBulkFailure(
                                failedItem(400, "document_parsing_exception")))
                .isFalse();
        assertThat(
                        Elasticsearch8AsyncWriter.isTransientBulkFailure(
                                failedItem(429, "es_rejected_execution_exception")))
                .isTrue();
        assertThat(
                        Elasticsearch8AsyncWriter.isTransientBulkFailure(
                                failedItem(503, "unavailable_shards_exception")))
                .isTrue();
        assertThat(
                        Elasticsearch8AsyncWriter.isTransientBulkFailure(
                                failedItem(400, "timeout_exception")))
                .isTrue();
    }

    @Test
    void classifiesNestedNetworkFailuresAsRetryable() {
        assertThat(
                        Elasticsearch8AsyncWriter.isTransientTransportFailure(
                                new CompletionException(new ConnectException("unavailable"))))
                .isTrue();
        assertThat(
                        Elasticsearch8AsyncWriter.isTransientTransportFailure(
                                new RuntimeException(new IOException("connection reset"))))
                .isTrue();
        assertThat(
                        Elasticsearch8AsyncWriter.isTransientTransportFailure(
                                new IllegalArgumentException("invalid request")))
                .isFalse();
    }

    private static BulkResponseItem failedItem(int status, String errorType) {
        return new BulkResponseItem.Builder()
                .operationType(OperationType.Index)
                .index("test-index")
                .id("test-id")
                .status(status)
                .error(error -> error.type(errorType).reason("test failure"))
                .build();
    }
}
