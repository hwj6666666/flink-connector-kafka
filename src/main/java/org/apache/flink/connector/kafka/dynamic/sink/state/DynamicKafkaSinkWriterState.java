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

package org.apache.flink.connector.kafka.dynamic.sink.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kafka.sink.KafkaWriterState;

import java.util.List;
import java.util.Objects;
import java.util.Properties;

/** State for a single resolved dynamic Kafka route. */
@Internal
public class DynamicKafkaSinkWriterState {

    private final String routeId;
    private final String kafkaClusterId;
    private final String topic;
    private final String transactionalIdPrefix;
    private final Properties kafkaProducerConfig;
    private final List<KafkaWriterState> kafkaWriterStates;

    public DynamicKafkaSinkWriterState(
            String routeId,
            String kafkaClusterId,
            String topic,
            String transactionalIdPrefix,
            Properties kafkaProducerConfig,
            List<KafkaWriterState> kafkaWriterStates) {
        this.routeId = routeId;
        this.kafkaClusterId = kafkaClusterId;
        this.topic = topic;
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.kafkaProducerConfig = kafkaProducerConfig;
        this.kafkaWriterStates = kafkaWriterStates;
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

    public List<KafkaWriterState> getKafkaWriterStates() {
        return kafkaWriterStates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DynamicKafkaSinkWriterState)) {
            return false;
        }
        DynamicKafkaSinkWriterState that = (DynamicKafkaSinkWriterState) o;
        return Objects.equals(routeId, that.routeId)
                && Objects.equals(kafkaClusterId, that.kafkaClusterId)
                && Objects.equals(topic, that.topic)
                && Objects.equals(transactionalIdPrefix, that.transactionalIdPrefix)
                && Objects.equals(kafkaProducerConfig, that.kafkaProducerConfig)
                && Objects.equals(kafkaWriterStates, that.kafkaWriterStates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                routeId,
                kafkaClusterId,
                topic,
                transactionalIdPrefix,
                kafkaProducerConfig,
                kafkaWriterStates);
    }
}
