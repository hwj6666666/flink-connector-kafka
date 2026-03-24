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

package org.apache.flink.connector.kafka.dynamic.sink.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;

import java.util.Objects;
import java.util.Properties;

/** Wrapper around a Kafka committable with the resolved dynamic route metadata. */
@Internal
public class DynamicKafkaCommittable {

    private final String routeId;
    private final String kafkaClusterId;
    private final String topic;
    private final String transactionalIdPrefix;
    private final Properties kafkaProducerConfig;
    private final KafkaCommittable kafkaCommittable;

    public DynamicKafkaCommittable(
            String routeId,
            String kafkaClusterId,
            String topic,
            String transactionalIdPrefix,
            Properties kafkaProducerConfig,
            KafkaCommittable kafkaCommittable) {
        this.routeId = routeId;
        this.kafkaClusterId = kafkaClusterId;
        this.topic = topic;
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.kafkaProducerConfig = kafkaProducerConfig;
        this.kafkaCommittable = kafkaCommittable;
    }

    public String getRouteId() {
        return routeId;
    }

    public String getKafkaClusterId() {
        return kafkaClusterId;
    }

    public String getTopic() {
        return topic;
    }

    public String getTransactionalIdPrefix() {
        return transactionalIdPrefix;
    }

    public Properties getKafkaProducerConfig() {
        return kafkaProducerConfig;
    }

    public KafkaCommittable getKafkaCommittable() {
        return kafkaCommittable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DynamicKafkaCommittable)) {
            return false;
        }
        DynamicKafkaCommittable that = (DynamicKafkaCommittable) o;
        return Objects.equals(routeId, that.routeId)
                && Objects.equals(kafkaClusterId, that.kafkaClusterId)
                && Objects.equals(topic, that.topic)
                && Objects.equals(transactionalIdPrefix, that.transactionalIdPrefix)
                && Objects.equals(kafkaProducerConfig, that.kafkaProducerConfig)
                && Objects.equals(kafkaCommittable, that.kafkaCommittable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                routeId,
                kafkaClusterId,
                topic,
                transactionalIdPrefix,
                kafkaProducerConfig,
                kafkaCommittable);
    }
}
