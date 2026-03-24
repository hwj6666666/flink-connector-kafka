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

package org.apache.flink.connector.kafka.sink.committer;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;
import org.apache.flink.connector.kafka.sink.internal.KafkaCommitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Committer that fans out commit requests to the per-route Kafka committer. */
public class DynamicKafkaCommitter implements Committer<DynamicKafkaCommittable> {

    private final DeliveryGuarantee deliveryGuarantee;
    private final CommitterInitContext context;
    private final TransactionNamingStrategy transactionNamingStrategy;
    private final Map<String, KafkaCommitter> committersByRouteId;

    public DynamicKafkaCommitter(
            DeliveryGuarantee deliveryGuarantee,
            CommitterInitContext context,
            TransactionNamingStrategy transactionNamingStrategy) {
        this.deliveryGuarantee = deliveryGuarantee;
        this.context = context;
        this.transactionNamingStrategy = transactionNamingStrategy;
        this.committersByRouteId = new LinkedHashMap<>();
    }

    @Override
    public void commit(Collection<CommitRequest<DynamicKafkaCommittable>> requests)
            throws IOException, InterruptedException {
        if (deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE) {
            return;
        }

        Map<String, List<CommitRequest<DynamicKafkaCommittable>>> requestsByRouteId =
                new LinkedHashMap<>();
        for (CommitRequest<DynamicKafkaCommittable> request : requests) {
            requestsByRouteId
                    .computeIfAbsent(
                            request.getCommittable().getRouteId(), ignored -> new ArrayList<>())
                    .add(request);
        }

        for (Map.Entry<String, List<CommitRequest<DynamicKafkaCommittable>>> entry :
                requestsByRouteId.entrySet()) {
            KafkaCommitter kafkaCommitter =
                    committersByRouteId.computeIfAbsent(
                            entry.getKey(),
                            ignored ->
                                    new KafkaCommitter(
                                            entry.getValue()
                                                    .get(0)
                                                    .getCommittable()
                                                    .getKafkaProducerConfig(),
                                            entry.getValue()
                                                    .get(0)
                                                    .getCommittable()
                                                    .getTransactionalIdPrefix(),
                                            context.getTaskInfo().getIndexOfThisSubtask(),
                                            context.getTaskInfo().getAttemptNumber(),
                                            transactionNamingStrategy
                                                    == TransactionNamingStrategy.POOLING,
                                            org.apache.flink.connector.kafka.sink.internal
                                                            .FlinkKafkaInternalProducer
                                                    ::new));

            List<CommitRequest<KafkaCommittable>> kafkaRequests = new ArrayList<>();
            for (CommitRequest<DynamicKafkaCommittable> request : entry.getValue()) {
                kafkaRequests.add(new DelegatingCommitRequest(request));
            }
            kafkaCommitter.commit(kafkaRequests);
        }
    }

    @Override
    public void close() throws Exception {
        Exception firstError = null;
        for (KafkaCommitter committer : committersByRouteId.values()) {
            try {
                committer.close();
            } catch (Exception error) {
                if (firstError == null) {
                    firstError = error;
                } else {
                    firstError.addSuppressed(error);
                }
            }
        }
        if (firstError != null) {
            throw firstError;
        }
    }

    private static class DelegatingCommitRequest implements CommitRequest<KafkaCommittable> {

        private final CommitRequest<DynamicKafkaCommittable> delegate;

        private DelegatingCommitRequest(CommitRequest<DynamicKafkaCommittable> delegate) {
            this.delegate = delegate;
        }

        @Override
        public KafkaCommittable getCommittable() {
            return delegate.getCommittable().getKafkaCommittable();
        }

        @Override
        public int getNumberOfRetries() {
            return delegate.getNumberOfRetries();
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {
            delegate.signalFailedWithKnownReason(t);
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {
            delegate.signalFailedWithUnknownReason(t);
        }

        @Override
        public void retryLater() {
            delegate.retryLater();
        }

        @Override
        public void updateAndRetryLater(KafkaCommittable committable) {
            throw new UnsupportedOperationException(
                    "KafkaCommitter does not update Kafka committables before retrying.");
        }

        @Override
        public void signalAlreadyCommitted() {
            delegate.signalAlreadyCommitted();
        }
    }
}
