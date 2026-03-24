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

package org.apache.flink.dynamic.sink.config;

import java.util.List;

/** Runtime config for the demo Flink job. */
public class FlinkConfig {

    private final String bootstrapServers;
    private final String streamPattern;
    private final String clusterId;
    private final List<String> records;

    public FlinkConfig(
            String bootstrapServers, String streamPattern, String clusterId, List<String> records) {
        this.bootstrapServers = bootstrapServers;
        this.streamPattern = streamPattern;
        this.clusterId = clusterId;
        this.records = records;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getStreamPattern() {
        return streamPattern;
    }

    public String getClusterId() {
        return clusterId;
    }

    public List<String> getRecords() {
        return records;
    }
}
