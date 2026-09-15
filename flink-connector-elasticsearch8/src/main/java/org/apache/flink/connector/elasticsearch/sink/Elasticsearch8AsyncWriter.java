/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Elasticsearch8AsyncWriter Apache Flink's Async Sink Writer that submits Operations into an
 * Elasticsearch cluster.
 *
 * @param <InputT> type of Operations
 */
public class Elasticsearch8AsyncWriter<InputT> extends AsyncSinkWriter<InputT, Operation> {
    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch8AsyncWriter.class);

    private final ElasticsearchAsyncClient esClient;

    private boolean close = false;

    private final Counter numRecordsOutErrorsCounter;

    /**
     * A counter to track number of records that are returned by Elasticsearch as failed and then
     * retried by this writer.
     */
    private final Counter numRecordsSendPartialFailureCounter;

    /** A counter to track the number of bulk requests that are sent to Elasticsearch. */
    private final Counter numRequestSubmittedCounter;

    private final OperationSerializer operationSerializer;

    public Elasticsearch8AsyncWriter(
            ElementConverter<InputT, Operation> elementConverter,
            WriterInitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            NetworkConfig networkConfig,
            Collection<BufferedRequestState<Operation>> state) {
        super(
                elementConverter,
                context,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(maxBatchSize)
                        .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                        .setMaxInFlightRequests(maxInFlightRequests)
                        .setMaxBufferedRequests(maxBufferedRequests)
                        .setMaxTimeInBufferMS(maxTimeInBufferMS)
                        .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                        .build(),
                state);

        this.esClient = networkConfig.createEsClient();
        final SinkWriterMetricGroup metricGroup = context.metricGroup();
        checkNotNull(metricGroup);

        this.numRecordsOutErrorsCounter = metricGroup.getNumRecordsOutErrorsCounter();
        this.numRecordsSendPartialFailureCounter =
                metricGroup.counter("numRecordsSendPartialFailure");
        this.numRequestSubmittedCounter = metricGroup.counter("numRequestSubmitted");
        this.operationSerializer = new OperationSerializer();
    }

    @Override
    protected void submitRequestEntries(
            List<Operation> requestEntries, ResultHandler<Operation> resultHandler) {
        numRequestSubmittedCounter.inc();
        LOG.debug("submitRequestEntries with {} items", requestEntries.size());

        BulkRequest.Builder br = new BulkRequest.Builder();
        for (Operation operation : requestEntries) {
            br.operations(new BulkOperation(operation.getBulkOperationVariant()));
        }

        esClient.bulk(br.build())
                .whenComplete(
                        (response, error) -> {
                            if (error != null) {
                                handleFailedRequest(requestEntries, resultHandler, error);
                            } else if (response.errors()) {
                                handlePartiallyFailedRequest(
                                        requestEntries, resultHandler, response);
                            } else {
                                handleSuccessfulRequest(resultHandler, response);
                            }
                        });
    }

    private void handleFailedRequest(
            List<Operation> requestEntries,
            ResultHandler<Operation> resultHandler,
            Throwable error) {
        Throwable rootCause = unwrap(error);
        LOG.warn(
                "Elasticsearch bulk transport failure: operations={}, errorType={}, reason={}",
                requestEntries.size(),
                rootCause.getClass().getName(),
                rootCause.getMessage(),
                rootCause);
        numRecordsOutErrorsCounter.inc(requestEntries.size());

        if (isTransientTransportFailure(rootCause)) {
            resultHandler.retryForEntries(requestEntries);
            return;
        }

        resultHandler.complete();
        getFatalExceptionCons()
                .accept(
                        new FlinkRuntimeException(
                                "Non-retryable Elasticsearch bulk transport failure", rootCause));
    }

    private void handlePartiallyFailedRequest(
            List<Operation> requestEntries,
            ResultHandler<Operation> resultHandler,
            BulkResponse response) {
        LOG.debug("The BulkRequest has failed partially. Response: {}", response);
        ArrayList<Operation> retryableItems = new ArrayList<>();
        BulkResponseItem firstFailedItem = null;
        BulkResponseItem firstDeterministicFailure = null;
        int failureCount = 0;
        for (int i = 0; i < response.items().size(); i++) {
            BulkResponseItem item = response.items().get(i);
            if (item.error() == null) {
                continue;
            }
            failureCount++;
            if (firstFailedItem == null) {
                firstFailedItem = item;
            }
            if (isTransientBulkFailure(item)) {
                retryableItems.add(requestEntries.get(i));
            } else if (firstDeterministicFailure == null) {
                firstDeterministicFailure = item;
            }
        }

        numRecordsOutErrorsCounter.inc(failureCount);
        numRecordsSendPartialFailureCounter.inc(retryableItems.size());
        LOG.info(
                "The BulkRequest with {} operation(s) has {} failure(s), of which {} are "
                        + "retryable. It took {}ms",
                requestEntries.size(),
                failureCount,
                retryableItems.size(),
                response.took());

        if (firstFailedItem != null) {
            logFailedBulkItem(firstFailedItem);
        }
        if (firstDeterministicFailure != null) {
            resultHandler.complete();
            getFatalExceptionCons()
                    .accept(
                            new FlinkRuntimeException(
                                    "Non-retryable Elasticsearch bulk item failure: "
                                            + failureSummary(firstDeterministicFailure)));
            return;
        }
        if (retryableItems.isEmpty()) {
            resultHandler.complete();
        } else {
            resultHandler.retryForEntries(retryableItems);
        }
    }

    private static void logFailedBulkItem(BulkResponseItem item) {
        LOG.error(
                "Elasticsearch bulk item failed: index={}, id={}, httpStatus={}, "
                        + "errorType={}, reason={}",
                item.index(),
                item.id(),
                item.status(),
                item.error().type(),
                item.error().reason());
    }

    private static String failureSummary(BulkResponseItem item) {
        return String.format(
                "index=%s, id=%s, httpStatus=%d, errorType=%s, reason=%s",
                item.index(), item.id(), item.status(), item.error().type(), item.error().reason());
    }

    static boolean isTransientBulkFailure(BulkResponseItem item) {
        int status = item.status();
        if (status == 408
                || status == 429
                || status == 500
                || status == 502
                || status == 503
                || status == 504) {
            return true;
        }
        String errorType =
                item.error().type() == null ? "" : item.error().type().toLowerCase(Locale.ROOT);
        return errorType.equals("es_rejected_execution_exception")
                || errorType.equals("timeout_exception")
                || errorType.equals("receive_timeout_transport_exception")
                || errorType.equals("connect_transport_exception")
                || errorType.equals("unavailable_shards_exception");
    }

    static boolean isTransientTransportFailure(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof IOException
                    || current instanceof ConnectException
                    || current instanceof NoRouteToHostException
                    || current instanceof SocketTimeoutException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private void handleSuccessfulRequest(
            ResultHandler<Operation> resultHandler, BulkResponse response) {
        LOG.debug(
                "The BulkRequest of {} operation(s) completed successfully. It took {}ms",
                response.items().size(),
                response.took());
        resultHandler.complete();
    }

    @Override
    protected long getSizeInBytes(Operation requestEntry) {
        return operationSerializer.size(requestEntry);
    }

    @Override
    public void close() {
        if (!close) {
            close = true;
            try {
                if (esClient != null && esClient._transport() != null) {
                    esClient._transport().close();
                }
            } catch (IOException e) {
                LOG.warn("Failed to close Elasticsearch transport during sink close", e);
            }
        }
    }
}
