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

package org.apache.flink.connector.kafka.dynamic.demo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.dynamic.metadata.SingleClusterTopicMetadataService;
import org.apache.flink.connector.kafka.dynamic.sink.DynamicKafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/** Minimal runnable demo job for {@link DynamicKafkaSink}. */
public class DynamicKafkaSinkDemoJob {

    public static void main(String[] args) throws Exception {
        Map<String, String> parameters = parseArgs(args);
        String bootstrapServers = getRequired(parameters, "bootstrap-servers");
        String streamPattern = getRequired(parameters, "stream-pattern");
        String clusterId = parameters.getOrDefault("cluster-id", "demo-cluster");
        String recordsArg = parameters.getOrDefault("records", "alpha,beta,gamma");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromData(Arrays.asList(recordsArg.split(",")))
                .sinkTo(
                        DynamicKafkaSink.<String>builder()
                                .setStreamPattern(Pattern.compile(streamPattern))
                                .setKafkaMetadataService(
                                        new SingleClusterTopicMetadataService(
                                                clusterId, properties))
                                .setRecordSerializer(
                                        KafkaRecordSerializationSchema.<String>builder()
                                                .setTopic("unused-by-dynamic-sink")
                                                .setValueSerializationSchema(
                                                        new SimpleStringSchema())
                                                .build())
                                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                                .setProperties(properties)
                                .build())
                .name("dynamic-kafka-sink-demo");

        env.execute("Dynamic Kafka Sink Demo");
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> parameters = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String key = args[i];
            if (!key.startsWith("--")) {
                continue;
            }
            String normalizedKey = key.substring(2);
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Missing value for argument --" + normalizedKey);
            }
            parameters.put(normalizedKey, args[++i]);
        }
        return parameters;
    }

    private static String getRequired(Map<String, String> parameters, String key) {
        String value = parameters.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Missing required argument --" + key);
        }
        return value;
    }
}
