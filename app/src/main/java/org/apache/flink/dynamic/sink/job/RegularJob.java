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

package org.apache.flink.dynamic.sink.job;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.dynamic.sink.util.ConfigUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/** Throughput benchmark job using regular KafkaSink. */
public class RegularJob {
    private static final String LETTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final int STRING_LENGTH = 8;
    private static final int EIGHT_DIGIT_BOUND = 100_000_000;
    private static final long DEFAULT_INTERVAL_MS = 0L;
    private static final int DEFAULT_PARALLELISM = 1;
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy:MM:dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        Map<String, String> parameters = ConfigUtil.parseArgs(args);
        String bootstrapServers = ConfigUtil.getRequired(parameters, "bootstrap-servers");
        String topic = ConfigUtil.getRequired(parameters, "topic");
        int parallelism =
                Integer.parseInt(parameters.getOrDefault("parallelism", String.valueOf(DEFAULT_PARALLELISM)));
        long intervalMs =
                Long.parseLong(parameters.getOrDefault("emit-interval-ms", String.valueOf(DEFAULT_INTERVAL_MS)));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        KafkaSink<String> sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.<String>builder()
                                        .setTopic(topic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();

        env.fromSource(
                        createRandomCsvSource(intervalMs),
                        WatermarkStrategy.noWatermarks(),
                        "random-csv-source")
                .sinkTo(sink)
                .name("regular-kafka-sink");
        env.execute("Regular Kafka Sink Benchmark Job");
    }

    private static DataGeneratorSource<String> createRandomCsvSource(long intervalMs) {
        GeneratorFunction<Long, String> generator = ignored -> generateRecord(ThreadLocalRandom.current());
        long recordsPerSecond = intervalMs <= 0 ? Long.MAX_VALUE : Math.max(1L, 1000L / intervalMs);
        return new DataGeneratorSource<>(
                generator,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(recordsPerSecond),
                TypeInformation.of(String.class));
    }

    private static String generateRecord(ThreadLocalRandom random) {
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMATTER);
        int number = random.nextInt(0, EIGHT_DIGIT_BOUND);
        StringBuilder word = new StringBuilder(STRING_LENGTH);
        for (int i = 0; i < STRING_LENGTH; i++) {
            int idx = random.nextInt(LETTERS.length());
            word.append(LETTERS.charAt(idx));
        }
        return timestamp + "," + String.format("%08d", number) + "," + word;
    }
}
