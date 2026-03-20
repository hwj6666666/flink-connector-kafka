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

package org.apache.flink.connector.kafka.dynamic.sink.serializer;

import org.apache.flink.connector.kafka.dynamic.sink.committer.DynamicKafkaCommittable;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Properties;

/** Serializer for {@link DynamicKafkaCommittable}. */
public class DynamicKafkaCommittableSerializer
        implements SimpleVersionedSerializer<DynamicKafkaCommittable> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DynamicKafkaCommittable committable) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(committable.getRouteId());
            out.writeUTF(committable.getKafkaClusterId());
            out.writeUTF(committable.getTopic());
            out.writeUTF(committable.getTransactionalIdPrefix());
            DynamicKafkaSinkSerializationUtils.writeProperties(
                    committable.getKafkaProducerConfig(), out);
            writeKafkaCommittable(committable.getKafkaCommittable(), out);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public DynamicKafkaCommittable deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            String routeId = in.readUTF();
            String kafkaClusterId = in.readUTF();
            String topic = in.readUTF();
            String transactionalIdPrefix = in.readUTF();
            Properties properties = DynamicKafkaSinkSerializationUtils.readProperties(in);
            KafkaCommittable kafkaCommittable = readKafkaCommittable(in);
            return new DynamicKafkaCommittable(
                    routeId,
                    kafkaClusterId,
                    topic,
                    transactionalIdPrefix,
                    properties,
                    kafkaCommittable);
        }
    }

    private static void writeKafkaCommittable(KafkaCommittable committable, DataOutputStream out)
            throws IOException {
        out.writeShort(committable.getEpoch());
        out.writeLong(committable.getProducerId());
        out.writeUTF(committable.getTransactionalId());
    }

    private static KafkaCommittable readKafkaCommittable(DataInputStream in) throws IOException {
        short epoch = in.readShort();
        long producerId = in.readLong();
        String transactionalId = in.readUTF();
        return new KafkaCommittable(producerId, epoch, transactionalId, null);
    }
}
