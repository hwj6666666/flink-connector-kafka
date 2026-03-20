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

import org.apache.flink.connector.kafka.testutils.KafkaUtil;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end demo test for {@link DynamicKafkaSinkDemoJob}. */
@Testcontainers
class DynamicKafkaSinkDemoITCase {

    @Container
    static final TestKafkaContainer KAFKA_CONTAINER =
            KafkaUtil.createKafkaContainer(DynamicKafkaSinkDemoITCase.class);

    @Test
    void testDemoJobWritesRecords() throws Exception {
        String topic = "dynamic-sink-demo-" + UUID.randomUUID();
        createTopic(topic);

        DynamicKafkaSinkDemoJob.main(
                new String[] {
                    "--bootstrap-servers",
                    KAFKA_CONTAINER.getBootstrapServers(),
                    "--topic",
                    topic,
                    "--records",
                    "delta,epsilon,zeta"
                });

        List<String> values =
                drainValues(topic).stream()
                        .map(record -> new String(record.value(), StandardCharsets.UTF_8))
                        .collect(Collectors.toList());

        assertThat(values).containsExactlyInAnyOrder("delta", "epsilon", "zeta");
    }

    private void createTopic(String topic) throws Exception {
        try (AdminClient adminClient = AdminClient.create(adminProperties())) {
            adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
    }

    private List<ConsumerRecord<byte[], byte[]>> drainValues(String topic) {
        Properties properties = new Properties();
        properties.putAll(adminProperties());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "dynamic-kafka-sink-demo");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return KafkaUtil.drainAllRecordsFromTopic(topic, properties);
    }

    private Map<String, Object> adminProperties() {
        return Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
    }
}
