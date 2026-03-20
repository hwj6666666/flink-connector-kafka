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

package org.apache.flink.connector.kafka.dynamic.sink;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.sink.committer.DynamicKafkaCommittable;
import org.apache.flink.connector.kafka.dynamic.sink.committer.DynamicKafkaCommitter;
import org.apache.flink.connector.kafka.dynamic.sink.serializer.DynamicKafkaCommittableSerializer;
import org.apache.flink.connector.kafka.dynamic.sink.state.DynamicKafkaSinkWriterState;
import org.apache.flink.connector.kafka.dynamic.sink.state.DynamicKafkaSinkWriterStateSerializer;
import org.apache.flink.connector.kafka.dynamic.sink.writer.DynamicKafkaSinkWriter;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSourceOptions;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber.KafkaStreamSubscriber;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;
import org.apache.flink.connector.kafka.sink.TwoPhaseCommittingStatefulSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

/**
 * Dynamic Kafka sink that resolves the target cluster and topic from {@link KafkaMetadataService}.
 *
 * <p>The current implementation supports exactly one logical stream at write time. Metadata updates
 * may switch the active physical route without restarting the Flink job.
 */
@Experimental
public class DynamicKafkaSink<IN>
        implements TwoPhaseCommittingStatefulSink<
                IN, DynamicKafkaSinkWriterState, DynamicKafkaCommittable> {

    private final KafkaStreamSubscriber kafkaStreamSubscriber;
    private final KafkaMetadataService kafkaMetadataService;
    private final KafkaRecordSerializationSchema<IN> recordSerializer;
    private final Properties properties;
    private final DeliveryGuarantee deliveryGuarantee;
    private final String transactionalIdPrefix;
    private final TransactionNamingStrategy transactionNamingStrategy;
    private final long metadataRefreshIntervalMs;

    DynamicKafkaSink(
            KafkaStreamSubscriber kafkaStreamSubscriber,
            KafkaMetadataService kafkaMetadataService,
            KafkaRecordSerializationSchema<IN> recordSerializer,
            Properties properties,
            DeliveryGuarantee deliveryGuarantee,
            String transactionalIdPrefix,
            TransactionNamingStrategy transactionNamingStrategy) {
        this.kafkaStreamSubscriber = kafkaStreamSubscriber;
        this.kafkaMetadataService = kafkaMetadataService;
        this.recordSerializer = recordSerializer;
        this.properties = properties;
        this.deliveryGuarantee = deliveryGuarantee;
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.transactionNamingStrategy = transactionNamingStrategy;
        this.metadataRefreshIntervalMs =
                DynamicKafkaSourceOptions.getOption(
                        properties,
                        DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS,
                        Long::parseLong);
    }

    public static <IN> DynamicKafkaSinkBuilder<IN> builder() {
        return new DynamicKafkaSinkBuilder<>();
    }

    @Override
    public PrecommittingStatefulSinkWriter<IN, DynamicKafkaSinkWriterState, DynamicKafkaCommittable>
            createWriter(WriterInitContext context) throws IOException {
        return new DynamicKafkaSinkWriter<>(
                context,
                kafkaStreamSubscriber,
                kafkaMetadataService,
                recordSerializer,
                properties,
                deliveryGuarantee,
                transactionalIdPrefix,
                transactionNamingStrategy,
                metadataRefreshIntervalMs,
                java.util.Collections.emptyList());
    }

    @Override
    public PrecommittingStatefulSinkWriter<IN, DynamicKafkaSinkWriterState, DynamicKafkaCommittable>
            restoreWriter(
                    WriterInitContext context,
                    Collection<DynamicKafkaSinkWriterState> recoveredState)
                    throws IOException {
        return new DynamicKafkaSinkWriter<>(
                context,
                kafkaStreamSubscriber,
                kafkaMetadataService,
                recordSerializer,
                properties,
                deliveryGuarantee,
                transactionalIdPrefix,
                transactionNamingStrategy,
                metadataRefreshIntervalMs,
                recoveredState);
    }

    @Override
    public Committer<DynamicKafkaCommittable> createCommitter(CommitterInitContext context)
            throws IOException {
        return new DynamicKafkaCommitter(deliveryGuarantee, context, transactionNamingStrategy);
    }

    @Override
    public SimpleVersionedSerializer<DynamicKafkaCommittable> getCommittableSerializer() {
        return new DynamicKafkaCommittableSerializer();
    }

    @Override
    public SimpleVersionedSerializer<DynamicKafkaSinkWriterState> getWriterStateSerializer() {
        return new DynamicKafkaSinkWriterStateSerializer();
    }
}
