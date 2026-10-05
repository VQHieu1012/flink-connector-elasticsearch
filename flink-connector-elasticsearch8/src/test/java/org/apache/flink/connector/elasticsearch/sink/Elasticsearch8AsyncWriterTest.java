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

import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;
import java.util.Map;
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

    @Test
    void ignorePolicyDropsOnlyVersionConflictsAndRetriesTransientFailures() {
        Operation conflicted = update("conflicted");
        Operation retryable = update("retryable");
        BulkResponse response =
                new BulkResponse.Builder()
                        .errors(true)
                        .took(1)
                        .items(
                                failedItem(
                                        OperationType.Update,
                                        "conflicted",
                                        409,
                                        "version_conflict_engine_exception"),
                                failedItem(
                                        OperationType.Update,
                                        "retryable",
                                        429,
                                        "es_rejected_execution_exception"))
                        .build();
        Elasticsearch8AsyncWriter.BulkFailureClassification result =
                Elasticsearch8AsyncWriter.classifyFailedResponse(
                        List.of(conflicted, retryable), response, VersionConflictPolicy.IGNORE);

        assertThat(result.retryableItems).containsExactly(retryable);
        assertThat(result.ignoredCount).isEqualTo(1);
        assertThat(result.firstDeterministicFailure).isNull();
    }

    @Test
    void failPolicyKeepsVersionConflictFatal() {
        BulkResponseItem conflict =
                failedItem(
                        OperationType.Update,
                        "conflicted",
                        409,
                        "version_conflict_engine_exception");
        BulkResponse response =
                new BulkResponse.Builder().errors(true).took(1).items(conflict).build();

        Elasticsearch8AsyncWriter.BulkFailureClassification result =
                Elasticsearch8AsyncWriter.classifyFailedResponse(
                        List.of(update("conflicted")), response, VersionConflictPolicy.FAIL);

        assertThat(result.firstDeterministicFailure).isEqualTo(conflict);
        assertThat(result.ignoredCount).isZero();
    }

    @Test
    void ignorePolicyCompletesWhenEveryFailureIsAnUpdateVersionConflict() {
        BulkResponseItem conflict =
                failedItem(
                        OperationType.Update,
                        "conflicted",
                        409,
                        "version_conflict_engine_exception");
        BulkResponse response =
                new BulkResponse.Builder().errors(true).took(1).items(conflict).build();

        Elasticsearch8AsyncWriter.BulkFailureClassification result =
                Elasticsearch8AsyncWriter.classifyFailedResponse(
                        List.of(update("conflicted")), response, VersionConflictPolicy.IGNORE);

        assertThat(result.retryableItems).isEmpty();
        assertThat(result.firstDeterministicFailure).isNull();
        assertThat(result.ignoredCount).isEqualTo(1);
        assertThat(result.failureCount).isEqualTo(1);
    }

    @Test
    void ignorePolicyDoesNotDropOtherConflicts() {
        BulkResponseItem indexConflict =
                failedItem(OperationType.Index, "index", 409, "version_conflict_engine_exception");
        BulkResponseItem otherUpdateConflict =
                failedItem(OperationType.Update, "update", 409, "other_conflict");
        BulkResponse response =
                new BulkResponse.Builder()
                        .errors(true)
                        .took(1)
                        .items(indexConflict, otherUpdateConflict)
                        .build();

        Elasticsearch8AsyncWriter.BulkFailureClassification result =
                Elasticsearch8AsyncWriter.classifyFailedResponse(
                        List.of(update("index"), update("update")),
                        response,
                        VersionConflictPolicy.IGNORE);

        assertThat(result.firstDeterministicFailure).isEqualTo(indexConflict);
        assertThat(result.ignoredCount).isZero();
    }

    private static Operation update(String id) {
        return new Operation(
                new UpdateOperation.Builder<Map<String, String>, Map<String, String>>()
                        .index("test-index")
                        .id(id)
                        .action(action -> action.doc(Map.of("value", id)))
                        .build());
    }

    private static BulkResponseItem failedItem(
            OperationType operationType, String id, int status, String errorType) {
        return new BulkResponseItem.Builder()
                .operationType(operationType)
                .index("test-index")
                .id(id)
                .status(status)
                .error(error -> error.type(errorType).reason("test failure"))
                .build();
    }

    private static BulkResponseItem failedItem(int status, String errorType) {
        return failedItem(OperationType.Index, "test-id", status, errorType);
    }
}
