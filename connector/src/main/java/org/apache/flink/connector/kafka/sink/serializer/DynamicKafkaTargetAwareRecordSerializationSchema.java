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

package org.apache.flink.connector.kafka.sink.serializer;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * Extension point for serializers that need the resolved target topic from the dynamic metadata.
 */
@Experimental
public interface DynamicKafkaTargetAwareRecordSerializationSchema<T>
        extends KafkaRecordSerializationSchema<T> {

    @Nullable
    ProducerRecord<byte[], byte[]> serialize(
            T element, KafkaSinkContext context, String targetTopic, Long timestamp);

    @Override
    default ProducerRecord<byte[], byte[]> serialize(
            T element, KafkaSinkContext context, Long timestamp) {
        throw new UnsupportedOperationException(
                "This serializer requires the resolved target topic.");
    }
}
