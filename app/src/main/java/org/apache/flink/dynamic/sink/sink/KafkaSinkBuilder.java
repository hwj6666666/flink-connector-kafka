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

package org.apache.flink.dynamic.sink.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.metadata.SingleClusterTopicMetadataService;
import org.apache.flink.connector.kafka.sink.DynamicKafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.dynamic.sink.job.DynamicSinkEvent;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.regex.Pattern;

/** Sink builder wrapper for job layer. */
public class KafkaSinkBuilder {

    public DynamicKafkaSink<DynamicSinkEvent> build(
            String bootstrapServers,
            String clusterId,
            String pattern,
            long discoveryIntervalMs) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(
                "stream-metadata-discovery-interval-ms", String.valueOf(discoveryIntervalMs));
        return DynamicKafkaSink.<DynamicSinkEvent>builder()
                .setStreamPattern(Pattern.compile(pattern))
                .setKafkaMetadataService(
                        new SingleClusterTopicMetadataService(clusterId, properties))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<DynamicSinkEvent>builder()
                                .setTopic("unused-by-dynamic-sink")
                                .setValueSerializationSchema(
                                        (SerializationSchema<DynamicSinkEvent>)
                                                event -> {
                                                    if (event == null || event.isRouteUpdate()) {
                                                        return null;
                                                    }
                                                    return event.getMessage() == null
                                                            ? null
                                                            : event.getMessage()
                                                                    .getBytes(StandardCharsets.UTF_8);
                                                })
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setProperties(properties)
                .build();
    }
}
