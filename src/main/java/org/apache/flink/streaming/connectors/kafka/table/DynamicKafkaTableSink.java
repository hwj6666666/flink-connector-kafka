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
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.sink.DynamicKafkaSink;
import org.apache.flink.connector.kafka.dynamic.sink.DynamicKafkaSinkBuilder;
import org.apache.flink.connector.kafka.sink.KafkaPartitioner;
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** Dynamic table sink backed by {@link DynamicKafkaSink}. */
@Internal
public class DynamicKafkaTableSink implements DynamicTableSink, SupportsWritingMetadata {

    protected List<String> metadataKeys;
    protected DataType consumedDataType;
    protected final DataType physicalDataType;
    protected final @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat;
    protected final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;
    protected final int[] keyProjection;
    protected final int[] valueProjection;
    protected final @Nullable String keyPrefix;
    protected final @Nullable List<String> streamIds;
    protected final @Nullable Pattern streamPattern;
    protected final KafkaMetadataService kafkaMetadataService;
    protected final Properties properties;
    protected final @Nullable KafkaPartitioner<RowData> partitioner;
    protected final DeliveryGuarantee deliveryGuarantee;
    protected final boolean upsertMode;
    protected final @Nullable Integer parallelism;
    protected final @Nullable String transactionalIdPrefix;
    protected final TransactionNamingStrategy transactionNamingStrategy;

    public DynamicKafkaTableSink(
            DataType consumedDataType,
            DataType physicalDataType,
            @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            @Nullable List<String> streamIds,
            @Nullable Pattern streamPattern,
            KafkaMetadataService kafkaMetadataService,
            Properties properties,
            @Nullable KafkaPartitioner<RowData> partitioner,
            DeliveryGuarantee deliveryGuarantee,
            boolean upsertMode,
            @Nullable Integer parallelism,
            @Nullable String transactionalIdPrefix,
            TransactionNamingStrategy transactionNamingStrategy) {
        this.consumedDataType = consumedDataType;
        this.physicalDataType = physicalDataType;
        this.keyEncodingFormat = keyEncodingFormat;
        this.valueEncodingFormat = valueEncodingFormat;
        this.keyProjection = keyProjection;
        this.valueProjection = valueProjection;
        this.keyPrefix = keyPrefix;
        this.streamIds = streamIds;
        this.streamPattern = streamPattern;
        this.kafkaMetadataService = kafkaMetadataService;
        this.properties = properties;
        this.partitioner = partitioner;
        this.deliveryGuarantee = deliveryGuarantee;
        this.upsertMode = upsertMode;
        this.parallelism = parallelism;
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.transactionNamingStrategy = transactionNamingStrategy;
        this.metadataKeys = java.util.Collections.emptyList();
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return valueEncodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> keySerialization =
                createSerialization(context, keyEncodingFormat, keyProjection, keyPrefix);
        final SerializationSchema<RowData> valueSerialization =
                createSerialization(context, valueEncodingFormat, valueProjection, null);
        final List<LogicalType> physicalChildren = physicalDataType.getLogicalType().getChildren();

        DynamicKafkaSinkBuilder<RowData> builder =
                DynamicKafkaSink.<RowData>builder()
                        .setKafkaMetadataService(kafkaMetadataService)
                        .setRecordSerializer(
                                new DynamicKafkaTableSinkSerializationSchema(
                                        partitioner,
                                        keySerialization,
                                        valueSerialization,
                                        getFieldGetters(physicalChildren, keyProjection),
                                        getFieldGetters(physicalChildren, valueProjection),
                                        getMetadataPositions(physicalChildren),
                                        upsertMode))
                        .setDeliveryGuarantee(deliveryGuarantee)
                        .setProperties(properties)
                        .setTransactionNamingStrategy(transactionNamingStrategy);

        if (streamIds != null) {
            builder.setStreamIds(new java.util.HashSet<>(streamIds));
        }
        if (streamPattern != null) {
            builder.setStreamPattern(streamPattern);
        }
        if (transactionalIdPrefix != null) {
            builder.setTransactionalIdPrefix(transactionalIdPrefix);
        }

        return SinkV2Provider.of(builder.build(), parallelism);
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        metadataMap.put(WritableMetadata.HEADERS.key, WritableMetadata.HEADERS.dataType);
        metadataMap.put(WritableMetadata.TIMESTAMP.key, WritableMetadata.TIMESTAMP.dataType);
        return metadataMap;
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.metadataKeys = metadataKeys;
        this.consumedDataType = consumedDataType;
    }

    @Override
    public DynamicTableSink copy() {
        DynamicKafkaTableSink copy =
                new DynamicKafkaTableSink(
                        consumedDataType,
                        physicalDataType,
                        keyEncodingFormat,
                        valueEncodingFormat,
                        keyProjection,
                        valueProjection,
                        keyPrefix,
                        streamIds,
                        streamPattern,
                        kafkaMetadataService,
                        properties,
                        partitioner,
                        deliveryGuarantee,
                        upsertMode,
                        parallelism,
                        transactionalIdPrefix,
                        transactionNamingStrategy);
        copy.metadataKeys = metadataKeys;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Dynamic Kafka table sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamicKafkaTableSink that = (DynamicKafkaTableSink) o;
        return Objects.equals(metadataKeys, that.metadataKeys)
                && Objects.equals(consumedDataType, that.consumedDataType)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(keyEncodingFormat, that.keyEncodingFormat)
                && Objects.equals(valueEncodingFormat, that.valueEncodingFormat)
                && Arrays.equals(keyProjection, that.keyProjection)
                && Arrays.equals(valueProjection, that.valueProjection)
                && Objects.equals(keyPrefix, that.keyPrefix)
                && Objects.equals(streamIds, that.streamIds)
                && Objects.equals(String.valueOf(streamPattern), String.valueOf(that.streamPattern))
                && Objects.equals(kafkaMetadataService, that.kafkaMetadataService)
                && Objects.equals(properties, that.properties)
                && Objects.equals(partitioner, that.partitioner)
                && Objects.equals(deliveryGuarantee, that.deliveryGuarantee)
                && Objects.equals(upsertMode, that.upsertMode)
                && Objects.equals(parallelism, that.parallelism)
                && Objects.equals(transactionalIdPrefix, that.transactionalIdPrefix)
                && Objects.equals(transactionNamingStrategy, that.transactionNamingStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                metadataKeys,
                consumedDataType,
                physicalDataType,
                keyEncodingFormat,
                valueEncodingFormat,
                Arrays.hashCode(keyProjection),
                Arrays.hashCode(valueProjection),
                keyPrefix,
                streamIds,
                String.valueOf(streamPattern),
                kafkaMetadataService,
                properties,
                partitioner,
                deliveryGuarantee,
                upsertMode,
                parallelism,
                transactionalIdPrefix,
                transactionNamingStrategy);
    }

    private int[] getMetadataPositions(List<LogicalType> physicalChildren) {
        return Stream.of(WritableMetadata.values())
                .mapToInt(
                        metadata -> {
                            final int position = metadataKeys.indexOf(metadata.key);
                            if (position < 0) {
                                return -1;
                            }
                            return physicalChildren.size() + position;
                        })
                .toArray();
    }

    private RowData.FieldGetter[] getFieldGetters(
            List<LogicalType> physicalChildren, int[] projection) {
        return Arrays.stream(projection)
                .mapToObj(
                        targetField ->
                                RowData.createFieldGetter(
                                        physicalChildren.get(targetField), targetField))
                .toArray(RowData.FieldGetter[]::new);
    }

    private @Nullable SerializationSchema<RowData> createSerialization(
            DynamicTableSink.Context context,
            @Nullable EncodingFormat<SerializationSchema<RowData>> format,
            int[] projection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType = Projection.of(projection).project(this.physicalDataType);
        if (prefix != null) {
            physicalFormatDataType =
                    TableDataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
        }
        return format.createRuntimeEncoder(context, physicalFormatDataType);
    }

    enum WritableMetadata {
        HEADERS(
                "headers",
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable())
                        .nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        final MapData map = row.getMap(pos);
                        final ArrayData keyArray = map.keyArray();
                        final ArrayData valueArray = map.valueArray();
                        final List<Header> headers = new ArrayList<>();
                        for (int i = 0; i < keyArray.size(); i++) {
                            if (!keyArray.isNullAt(i) && !valueArray.isNullAt(i)) {
                                headers.add(
                                        new HeaderImpl(
                                                keyArray.getString(i).toString(),
                                                valueArray.getBinary(i)));
                            }
                        }
                        return headers;
                    }
                }),
        TIMESTAMP(
                "timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        return row.getTimestamp(pos, 3).getMillisecond();
                    }
                });

        final String key;
        final DataType dataType;
        final MetadataConverter converter;

        WritableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }

    interface MetadataConverter extends Serializable {
        Object read(RowData consumedRow, int pos);
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
