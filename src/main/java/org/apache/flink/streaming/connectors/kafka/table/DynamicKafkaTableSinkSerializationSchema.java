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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.dynamic.sink.serializer.DynamicKafkaTargetAwareRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaPartitioner;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaRecordSerializationSchema.createProjectedRow;

/** RowData serializer for {@link DynamicKafkaTableSink}. */
@Internal
class DynamicKafkaTableSinkSerializationSchema
        implements DynamicKafkaTargetAwareRecordSerializationSchema<RowData> {

    @Nullable private final KafkaPartitioner<RowData> partitioner;
    @Nullable private final SerializationSchema<RowData> keySerialization;
    private final SerializationSchema<RowData> valueSerialization;
    private final RowData.FieldGetter[] keyFieldGetters;
    private final RowData.FieldGetter[] valueFieldGetters;
    private final int[] metadataPositions;
    private final boolean upsertMode;

    DynamicKafkaTableSinkSerializationSchema(
            @Nullable KafkaPartitioner<RowData> partitioner,
            @Nullable SerializationSchema<RowData> keySerialization,
            SerializationSchema<RowData> valueSerialization,
            RowData.FieldGetter[] keyFieldGetters,
            RowData.FieldGetter[] valueFieldGetters,
            int[] metadataPositions,
            boolean upsertMode) {
        this.partitioner = partitioner;
        this.keySerialization = keySerialization;
        this.valueSerialization = valueSerialization;
        this.keyFieldGetters = keyFieldGetters;
        this.valueFieldGetters = valueFieldGetters;
        this.metadataPositions = metadataPositions;
        this.upsertMode = upsertMode;
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
            throws Exception {
        if (keySerialization != null) {
            keySerialization.open(context);
        }
        valueSerialization.open(context);
        if (partitioner != null) {
            partitioner.open(
                    sinkContext.getParallelInstanceId(),
                    sinkContext.getNumberOfParallelInstances());
        }
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            RowData consumedRow, KafkaSinkContext context, String targetTopic, Long timestamp) {
        final byte[] keySerialized;
        if (keySerialization == null) {
            keySerialized = null;
        } else {
            RowData keyRow = createProjectedRow(consumedRow, RowKind.INSERT, keyFieldGetters);
            keySerialized = keySerialization.serialize(keyRow);
        }

        final byte[] valueSerialized;
        RowKind kind = consumedRow.getRowKind();
        if (upsertMode) {
            if (kind == RowKind.DELETE || kind == RowKind.UPDATE_BEFORE) {
                valueSerialized = null;
            } else {
                RowData valueRow = createProjectedRow(consumedRow, kind, valueFieldGetters);
                valueRow.setRowKind(RowKind.INSERT);
                valueSerialized = valueSerialization.serialize(valueRow);
            }
        } else {
            RowData valueRow = createProjectedRow(consumedRow, kind, valueFieldGetters);
            valueSerialized = valueSerialization.serialize(valueRow);
        }

        return new ProducerRecord<>(
                targetTopic,
                extractPartition(
                        consumedRow,
                        targetTopic,
                        keySerialized,
                        valueSerialized,
                        context.getPartitionsForTopic(targetTopic)),
                readMetadata(consumedRow, KafkaDynamicSink.WritableMetadata.TIMESTAMP),
                keySerialized,
                valueSerialized,
                readMetadata(consumedRow, KafkaDynamicSink.WritableMetadata.HEADERS));
    }

    @SuppressWarnings("unchecked")
    private <T> T readMetadata(RowData consumedRow, KafkaDynamicSink.WritableMetadata metadata) {
        int position = metadataPositions[metadata.ordinal()];
        if (position < 0) {
            return null;
        }
        switch (metadata) {
            case TIMESTAMP:
                if (consumedRow.isNullAt(position)) {
                    return null;
                }
                return (T) Long.valueOf(consumedRow.getTimestamp(position, 3).getMillisecond());
            case HEADERS:
                if (consumedRow.isNullAt(position)) {
                    return null;
                }
                MapData map = consumedRow.getMap(position);
                ArrayData keyArray = map.keyArray();
                ArrayData valueArray = map.valueArray();
                List<Header> headers = new ArrayList<>();
                for (int i = 0; i < keyArray.size(); i++) {
                    if (!keyArray.isNullAt(i) && !valueArray.isNullAt(i)) {
                        headers.add(
                                new HeaderImpl(
                                        keyArray.getString(i).toString(), valueArray.getBinary(i)));
                    }
                }
                return (T) headers;
            default:
                return null;
        }
    }

    private Integer extractPartition(
            RowData consumedRow,
            String targetTopic,
            @Nullable byte[] keySerialized,
            @Nullable byte[] valueSerialized,
            int[] partitions) {
        if (partitioner == null) {
            return null;
        }
        return partitioner.partition(
                consumedRow, keySerialized, valueSerialized, targetTopic, partitions);
    }

    private static class HeaderImpl implements Header {
        private final String key;
        private final byte[] value;

        private HeaderImpl(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public byte[] value() {
            return value;
        }
    }
}
