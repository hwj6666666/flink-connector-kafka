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

import org.apache.flink.dynamic.sink.config.FlinkConfig;
import org.apache.flink.dynamic.sink.process.AggregateFunction;
import org.apache.flink.dynamic.sink.process.EnrichFunction;
import org.apache.flink.dynamic.sink.process.OrderProcessFunction;
import org.apache.flink.dynamic.sink.sink.KafkaSinkBuilder;
import org.apache.flink.dynamic.sink.source.KafkaSourceBuilder;
import org.apache.flink.dynamic.sink.util.ConfigUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Map;

/** Flink job entrypoint. */
public class OrderJob {

    public static void main(String[] args) throws Exception {
        FlinkConfig config = loadConfig(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        new KafkaSourceBuilder()
                .fromRecords(env, config.getRecords())
                .map(new OrderProcessFunction())
                .map(new EnrichFunction())
                .map(new AggregateFunction())
                .sinkTo(
                        new KafkaSinkBuilder()
                                .build(
                                        config.getBootstrapServers(),
                                        config.getClusterId(),
                                        config.getStreamPattern()))
                .name("dynamic-kafka-sink");

        env.execute("Order Job");
    }

    private static FlinkConfig loadConfig(String[] args) {
        Map<String, String> parameters = ConfigUtil.parseArgs(args);
        String bootstrapServers = ConfigUtil.getRequired(parameters, "bootstrap-servers");
        String streamPattern = ConfigUtil.getRequired(parameters, "stream-pattern");
        String clusterId = parameters.getOrDefault("cluster-id", "default-cluster");
        String recordsArg = parameters.getOrDefault("records", "alpha,beta,gamma");
        return new FlinkConfig(
                bootstrapServers, streamPattern, clusterId, Arrays.asList(recordsArg.split(",")));
    }
}
