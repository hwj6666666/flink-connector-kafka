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
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.sink.subscriber.KafkaStreamSetSubscriber;
import org.apache.flink.connector.kafka.dynamic.sink.subscriber.KafkaStreamSubscriber;
import org.apache.flink.connector.kafka.dynamic.sink.subscriber.StreamPatternSubscriber;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.CommonClientConfigs;

import javax.annotation.Nullable;

import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/** Builder for {@link DynamicKafkaSink}. */
@Experimental
public class DynamicKafkaSinkBuilder<T> {

    private KafkaStreamSubscriber kafkaStreamSubscriber;
    private KafkaMetadataService kafkaMetadataService;
    private KafkaRecordSerializationSchema<T> recordSerializer;
    private final Properties properties;
    private DeliveryGuarantee deliveryGuarantee;
    @Nullable private String transactionalIdPrefix;
    private TransactionNamingStrategy transactionNamingStrategy;

    DynamicKafkaSinkBuilder() {
        this.properties = new Properties();
        this.deliveryGuarantee = DeliveryGuarantee.NONE;
        this.transactionNamingStrategy = TransactionNamingStrategy.DEFAULT;
    }

    public DynamicKafkaSinkBuilder<T> setStreamId(String streamId) {
        return setStreamIds(Set.of(streamId));
    }

    public DynamicKafkaSinkBuilder<T> setStreamIds(Set<String> streamIds) {
        Preconditions.checkNotNull(streamIds);
        ensureSubscriberIsNull("stream ids");
        this.kafkaStreamSubscriber = new KafkaStreamSetSubscriber(streamIds);
        return this;
    }

    public DynamicKafkaSinkBuilder<T> setStreamPattern(Pattern streamPattern) {
        Preconditions.checkNotNull(streamPattern);
        ensureSubscriberIsNull("stream pattern");
        this.kafkaStreamSubscriber = new StreamPatternSubscriber(streamPattern);
        return this;
    }

    public DynamicKafkaSinkBuilder<T> setKafkaStreamSubscriber(
            KafkaStreamSubscriber kafkaStreamSubscriber) {
        Preconditions.checkNotNull(kafkaStreamSubscriber);
        ensureSubscriberIsNull("custom subscriber");
        this.kafkaStreamSubscriber = kafkaStreamSubscriber;
        return this;
    }

    public DynamicKafkaSinkBuilder<T> setKafkaMetadataService(
            KafkaMetadataService kafkaMetadataService) {
        this.kafkaMetadataService = kafkaMetadataService;
        return this;
    }

    public DynamicKafkaSinkBuilder<T> setRecordSerializer(
            KafkaRecordSerializationSchema<T> recordSerializer) {
        this.recordSerializer = recordSerializer;
        return this;
    }

    public DynamicKafkaSinkBuilder<T> setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = deliveryGuarantee;
        return this;
    }

    public DynamicKafkaSinkBuilder<T> setTransactionalIdPrefix(String transactionalIdPrefix) {
        this.transactionalIdPrefix = transactionalIdPrefix;
        return this;
    }

    public DynamicKafkaSinkBuilder<T> setTransactionNamingStrategy(
            TransactionNamingStrategy transactionNamingStrategy) {
        this.transactionNamingStrategy = transactionNamingStrategy;
        return this;
    }

    public DynamicKafkaSinkBuilder<T> setProperties(Properties properties) {
        this.properties.putAll(properties);
        return this;
    }

    public DynamicKafkaSinkBuilder<T> setProperty(String key, String value) {
        this.properties.setProperty(key, value);
        return this;
    }

    public DynamicKafkaSinkBuilder<T> setClientIdPrefix(String clientIdPrefix) {
        return setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, clientIdPrefix);
    }

    public DynamicKafkaSink<T> build() {
        sanityCheck();
        return new DynamicKafkaSink<>(
                kafkaStreamSubscriber,
                kafkaMetadataService,
                recordSerializer,
                properties,
                deliveryGuarantee,
                getOrCreateTransactionalIdPrefix(),
                transactionNamingStrategy);
    }

    private void sanityCheck() {
        Preconditions.checkNotNull(
                kafkaStreamSubscriber, "Kafka stream subscriber is required but not provided.");
        Preconditions.checkNotNull(
                kafkaMetadataService, "Kafka metadata service is required but not provided.");
        Preconditions.checkNotNull(
                recordSerializer, "Record serialization schema is required but not provided.");
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            Preconditions.checkState(
                    transactionalIdPrefix != null && !transactionalIdPrefix.isEmpty(),
                    "EXACTLY_ONCE delivery guarantee requires a transactionalIdPrefix.");
        }
    }

    private String getOrCreateTransactionalIdPrefix() {
        if (transactionalIdPrefix != null) {
            return transactionalIdPrefix;
        }
        return "DynamicKafkaSink-" + RandomStringUtils.randomAlphabetic(8);
    }

    private void ensureSubscriberIsNull(String attemptingSubscribeMode) {
        if (kafkaStreamSubscriber != null) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot use %s because a %s is already configured.",
                            attemptingSubscribeMode,
                            kafkaStreamSubscriber.getClass().getSimpleName()));
        }
    }
}
