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

package org.apache.flink.connector.kafka.sink.writer;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.metadata.KafkaStream;
import org.apache.flink.connector.kafka.sink.committer.DynamicKafkaCommittable;
import org.apache.flink.connector.kafka.sink.serializer.DynamicKafkaTargetAwareRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.state.DynamicKafkaSinkWriterState;
import org.apache.flink.connector.kafka.sink.subscriber.KafkaStreamSubscriber;
import org.apache.flink.connector.kafka.sink.lineage.DefaultKafkaDatasetFacet;
import org.apache.flink.connector.kafka.sink.lineage.DefaultKafkaDatasetIdentifier;
import org.apache.flink.connector.kafka.sink.lineage.KafkaDatasetFacet;
import org.apache.flink.connector.kafka.sink.lineage.KafkaDatasetFacetProvider;
import org.apache.flink.connector.kafka.sink.lineage.TypeDatasetFacet;
import org.apache.flink.connector.kafka.sink.lineage.TypeDatasetFacetProvider;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaWriterState;
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;
import org.apache.flink.connector.kafka.sink.TwoPhaseCommittingStatefulSink;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/** Writer implementation for the dynamic Kafka sink runtime. */
public class DynamicKafkaSinkWriter<IN>
        implements TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<
                IN, DynamicKafkaSinkWriterState, DynamicKafkaCommittable> {

    private final WriterInitContext context;
    private final KafkaStreamSubscriber kafkaStreamSubscriber;
    private final KafkaMetadataService kafkaMetadataService;
    private final KafkaRecordSerializationSchema<IN> recordSerializer;
    private final Properties commonProperties;
    private final DeliveryGuarantee deliveryGuarantee;
    private final String transactionalIdPrefix;
    private final TransactionNamingStrategy transactionNamingStrategy;
    private final long metadataRefreshIntervalMs;
    private final Map<String, ClusterWriter<IN>> clusterWritersByRouteId;

    private Set<String> activeRouteIds;
    private long nextMetadataRefreshTimestamp;
    private boolean closed;

    public DynamicKafkaSinkWriter(
            WriterInitContext context,
            KafkaStreamSubscriber kafkaStreamSubscriber,
            KafkaMetadataService kafkaMetadataService,
            KafkaRecordSerializationSchema<IN> recordSerializer,
            Properties commonProperties,
            DeliveryGuarantee deliveryGuarantee,
            String transactionalIdPrefix,
            TransactionNamingStrategy transactionNamingStrategy,
            long metadataRefreshIntervalMs,
            Collection<DynamicKafkaSinkWriterState> recoveredStates)
            throws IOException {
        this.context = context;
        this.kafkaStreamSubscriber = kafkaStreamSubscriber;
        this.kafkaMetadataService = kafkaMetadataService;
        this.recordSerializer = recordSerializer;
        this.commonProperties = commonProperties;
        this.deliveryGuarantee = deliveryGuarantee;
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.transactionNamingStrategy = transactionNamingStrategy;
        this.metadataRefreshIntervalMs = metadataRefreshIntervalMs;
        this.clusterWritersByRouteId = new LinkedHashMap<>();
        this.nextMetadataRefreshTimestamp = Long.MIN_VALUE;
        this.activeRouteIds = Collections.emptySet();

        for (DynamicKafkaSinkWriterState recoveredState : recoveredStates) {
            clusterWritersByRouteId.put(
                    recoveredState.getRouteId(),
                    createClusterWriter(
                            recoveredState.getRouteId(),
                            recoveredState.getKafkaClusterId(),
                            recoveredState.getTopic(),
                            recoveredState.getTransactionalIdPrefix(),
                            recoveredState.getKafkaProducerConfig(),
                            recoveredState.getKafkaWriterStates()));
        }

        refreshRouteIfNeeded(true);
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        refreshRouteIfNeeded(false);
        Preconditions.checkState(
                !activeRouteIds.isEmpty(), "No active routes resolved for DynamicKafkaSink.");
        for (String routeId : activeRouteIds) {
            ClusterWriter<IN> clusterWriter = clusterWritersByRouteId.get(routeId);
            Preconditions.checkState(
                    clusterWriter != null, "No writer created for active route '%s'.", routeId);
            Preconditions.checkNotNull(clusterWriter).writer.write(element, context);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        refreshRouteIfNeeded(false);
        for (ClusterWriter<IN> clusterWriter : clusterWritersByRouteId.values()) {
            clusterWriter.writer.flush(endOfInput);
        }
    }

    @Override
    public Collection<DynamicKafkaCommittable> prepareCommit()
            throws IOException, InterruptedException {
        List<DynamicKafkaCommittable> committables = new ArrayList<>();
        for (ClusterWriter<IN> clusterWriter : clusterWritersByRouteId.values()) {
            for (KafkaCommittable kafkaCommittable : clusterWriter.writer.prepareCommit()) {
                committables.add(
                        new DynamicKafkaCommittable(
                                clusterWriter.routeId,
                                clusterWriter.kafkaClusterId,
                                clusterWriter.topic,
                                clusterWriter.transactionalIdPrefix,
                                clusterWriter.kafkaProducerConfig,
                                kafkaCommittable));
            }
        }
        return committables;
    }

    @Override
    public List<DynamicKafkaSinkWriterState> snapshotState(long checkpointId) throws IOException {
        refreshRouteIfNeeded(false);
        List<DynamicKafkaSinkWriterState> states = new ArrayList<>();
        for (ClusterWriter<IN> clusterWriter : clusterWritersByRouteId.values()) {
            List<KafkaWriterState> kafkaStates = clusterWriter.writer.snapshotState(checkpointId);
            if (!kafkaStates.isEmpty()) {
                states.add(
                        new DynamicKafkaSinkWriterState(
                                clusterWriter.routeId,
                                clusterWriter.kafkaClusterId,
                                clusterWriter.topic,
                                clusterWriter.transactionalIdPrefix,
                                clusterWriter.kafkaProducerConfig,
                                kafkaStates));
            }
        }
        return states;
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;

        Exception firstError = null;
        for (ClusterWriter<IN> clusterWriter : clusterWritersByRouteId.values()) {
            try {
                clusterWriter.writer.close();
            } catch (Exception error) {
                if (firstError == null) {
                    firstError = error;
                } else {
                    firstError.addSuppressed(error);
                }
            }
        }
        try {
            kafkaMetadataService.close();
        } catch (Exception error) {
            if (firstError == null) {
                firstError = error;
            } else {
                firstError.addSuppressed(error);
            }
        }
        if (firstError != null) {
            throw firstError;
        }
    }

    private void refreshRouteIfNeeded(boolean force) throws IOException {
        long now = System.currentTimeMillis();
        if (!force) {
            if (metadataRefreshIntervalMs <= 0 && !activeRouteIds.isEmpty()) {
                return;
            }
            if (metadataRefreshIntervalMs > 0 && now < nextMetadataRefreshTimestamp) {
                return;
            }
        }

        List<ResolvedRoute> routes = resolveRoutes();
        activeRouteIds = routes.stream().map(route -> route.routeId).collect(Collectors.toSet());
        nextMetadataRefreshTimestamp =
                metadataRefreshIntervalMs > 0 ? now + metadataRefreshIntervalMs : Long.MAX_VALUE;

        for (ResolvedRoute route : routes) {
            clusterWritersByRouteId.computeIfAbsent(
                    route.routeId,
                    ignored -> {
                        try {
                            return createClusterWriter(
                                    route.routeId,
                                    route.kafkaClusterId,
                                    route.topic,
                                    route.transactionalIdPrefix,
                                    route.kafkaProducerConfig,
                                    Collections.emptyList());
                        } catch (IOException error) {
                            throw new RuntimeException(error);
                        }
                    });
        }
    }

    private List<ResolvedRoute> resolveRoutes() {
        Collection<KafkaStream> subscribedStreams =
                kafkaStreamSubscriber.getSubscribedStreams(kafkaMetadataService);
        Preconditions.checkState(
                !subscribedStreams.isEmpty(),
                "DynamicKafkaSink requires at least one stream, but found none for current subscriber.");

        List<ResolvedRoute> routes = new ArrayList<>();
        for (KafkaStream stream : subscribedStreams) {
            List<Map.Entry<String, ClusterMetadata>> activeClusters =
                    stream.getClusterMetadataMap().entrySet().stream()
                            .filter(entry -> kafkaMetadataService.isClusterActive(entry.getKey()))
                            .collect(Collectors.toList());

            Preconditions.checkState(
                    !activeClusters.isEmpty(),
                    "DynamicKafkaSink requires at least one active cluster for stream '%s'.",
                    stream.getStreamId());

            for (Map.Entry<String, ClusterMetadata> clusterEntry : activeClusters) {
                String kafkaClusterId = clusterEntry.getKey();
                Preconditions.checkState(
                        !clusterEntry.getValue().getTopics().isEmpty(),
                        "DynamicKafkaSink requires at least one topic for stream '%s' on cluster '%s'.",
                        stream.getStreamId(),
                        kafkaClusterId);

                for (String topic : clusterEntry.getValue().getTopics()) {
                    Properties kafkaProducerConfig = new Properties();
                    kafkaProducerConfig.putAll(commonProperties);
                    kafkaProducerConfig.putAll(clusterEntry.getValue().getProperties());

                    String bootstrapServers =
                            kafkaProducerConfig.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
                    Preconditions.checkState(
                            bootstrapServers != null && !bootstrapServers.isEmpty(),
                            "DynamicKafkaSink requires bootstrap servers for cluster '%s'.",
                            kafkaClusterId);

                    String routeId = kafkaClusterId + "|" + topic + "|" + bootstrapServers;
                    routes.add(
                            new ResolvedRoute(
                                    routeId,
                                    kafkaClusterId,
                                    topic,
                                    buildTransactionalIdPrefix(routeId),
                                    kafkaProducerConfig));
                }
            }
        }

        Preconditions.checkState(
                !routes.isEmpty(),
                "DynamicKafkaSink could not resolve any writable routes from current metadata. Found streams: %s",
                subscribedStreams.stream()
                        .map(KafkaStream::getStreamId)
                        .collect(Collectors.toList()));
        return routes;
    }

    private ClusterWriter<IN> createClusterWriter(
            String routeId,
            String kafkaClusterId,
            String topic,
            String routeTransactionalIdPrefix,
            Properties kafkaProducerConfig,
            Collection<KafkaWriterState> recoveredStates)
            throws IOException {
        KafkaRecordSerializationSchema<IN> clusterSerializer = cloneSerializerForRoute(topic);
        @SuppressWarnings("unchecked")
        TwoPhaseCommittingStatefulSink<IN, KafkaWriterState, KafkaCommittable> clusterSink =
                (TwoPhaseCommittingStatefulSink<IN, KafkaWriterState, KafkaCommittable>)
                        KafkaSink.<IN>builder()
                                .setDeliveryGuarantee(deliveryGuarantee)
                                .setKafkaProducerConfig(kafkaProducerConfig)
                                .setBootstrapServers(
                                        kafkaProducerConfig.getProperty(
                                                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))
                                .setRecordSerializer(clusterSerializer)
                                .setTransactionNamingStrategy(transactionNamingStrategy)
                                .setTransactionalIdPrefix(routeTransactionalIdPrefix)
                                .build();

        TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<
                        IN, KafkaWriterState, KafkaCommittable>
                writer =
                        recoveredStates.isEmpty()
                                ? clusterSink.createWriter(context)
                                : clusterSink.restoreWriter(context, recoveredStates);

        return new ClusterWriter<>(
                routeId,
                kafkaClusterId,
                topic,
                routeTransactionalIdPrefix,
                kafkaProducerConfig,
                writer);
    }

    private KafkaRecordSerializationSchema<IN> cloneSerializerForRoute(String topic)
            throws IOException {
        try {
            KafkaRecordSerializationSchema<IN> serializerCopy =
                    InstantiationUtil.clone(
                            recordSerializer, context.getUserCodeClassLoader().asClassLoader());
            return new FixedTopicKafkaRecordSerializationSchema<>(serializerCopy, topic);
        } catch (Exception error) {
            throw new IOException(
                    "Failed to clone record serializer for dynamic Kafka sink.", error);
        }
    }

    private String buildTransactionalIdPrefix(String routeId) {
        return transactionalIdPrefix + "-" + Integer.toUnsignedString(routeId.hashCode());
    }

    private static class ResolvedRoute {
        private final String routeId;
        private final String kafkaClusterId;
        private final String topic;
        private final String transactionalIdPrefix;
        private final Properties kafkaProducerConfig;

        private ResolvedRoute(
                String routeId,
                String kafkaClusterId,
                String topic,
                String transactionalIdPrefix,
                Properties kafkaProducerConfig) {
            this.routeId = routeId;
            this.kafkaClusterId = kafkaClusterId;
            this.topic = topic;
            this.transactionalIdPrefix = transactionalIdPrefix;
            this.kafkaProducerConfig = kafkaProducerConfig;
        }
    }

    private static class ClusterWriter<IN> {
        private final String routeId;
        private final String kafkaClusterId;
        private final String topic;
        private final String transactionalIdPrefix;
        private final Properties kafkaProducerConfig;
        private final TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<
                        IN, KafkaWriterState, KafkaCommittable>
                writer;

        private ClusterWriter(
                String routeId,
                String kafkaClusterId,
                String topic,
                String transactionalIdPrefix,
                Properties kafkaProducerConfig,
                TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<
                                IN, KafkaWriterState, KafkaCommittable>
                        writer) {
            this.routeId = routeId;
            this.kafkaClusterId = kafkaClusterId;
            this.topic = topic;
            this.transactionalIdPrefix = transactionalIdPrefix;
            this.kafkaProducerConfig = kafkaProducerConfig;
            this.writer = writer;
        }
    }

    private static class FixedTopicKafkaRecordSerializationSchema<T>
            implements KafkaRecordSerializationSchema<T>,
                    KafkaDatasetFacetProvider,
                    TypeDatasetFacetProvider {

        private final KafkaRecordSerializationSchema<T> delegate;
        private final String topic;

        private FixedTopicKafkaRecordSerializationSchema(
                KafkaRecordSerializationSchema<T> delegate, String topic) {
            this.delegate = delegate;
            this.topic = topic;
        }

        @Override
        public void open(
                SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
                throws Exception {
            delegate.open(context, sinkContext);
        }

        @Nullable
        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                T element, KafkaSinkContext context, Long timestamp) {
            if (delegate instanceof DynamicKafkaTargetAwareRecordSerializationSchema) {
                return ((DynamicKafkaTargetAwareRecordSerializationSchema<T>) delegate)
                        .serialize(element, context, topic, timestamp);
            }

            ProducerRecord<byte[], byte[]> record = delegate.serialize(element, context, timestamp);
            if (record == null) {
                return null;
            }
            return new ProducerRecord<>(
                    topic,
                    record.partition(),
                    record.timestamp(),
                    record.key(),
                    record.value(),
                    record.headers());
        }

        @Override
        public Optional<KafkaDatasetFacet> getKafkaDatasetFacet() {
            return Optional.of(
                    new DefaultKafkaDatasetFacet(
                            DefaultKafkaDatasetIdentifier.ofTopics(
                                    Collections.singletonList(topic))));
        }

        @Override
        public Optional<TypeDatasetFacet> getTypeDatasetFacet() {
            if (delegate instanceof TypeDatasetFacetProvider) {
                return ((TypeDatasetFacetProvider) delegate).getTypeDatasetFacet();
            }
            return Optional.empty();
        }
    }
}
