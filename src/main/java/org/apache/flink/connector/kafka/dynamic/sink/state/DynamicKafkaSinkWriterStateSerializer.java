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

package org.apache.flink.connector.kafka.dynamic.sink.state;

import org.apache.flink.connector.kafka.dynamic.sink.serializer.DynamicKafkaSinkSerializationUtils;
import org.apache.flink.connector.kafka.sink.KafkaWriterState;
import org.apache.flink.connector.kafka.sink.internal.CheckpointTransaction;
import org.apache.flink.connector.kafka.sink.internal.TransactionOwnership;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** Serializer for {@link DynamicKafkaSinkWriterState}. */
public class DynamicKafkaSinkWriterStateSerializer
        implements SimpleVersionedSerializer<DynamicKafkaSinkWriterState> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DynamicKafkaSinkWriterState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(state.getRouteId());
            out.writeUTF(state.getKafkaClusterId());
            out.writeUTF(state.getTopic());
            out.writeUTF(state.getTransactionalIdPrefix());
            DynamicKafkaSinkSerializationUtils.writeProperties(state.getKafkaProducerConfig(), out);
            out.writeInt(state.getKafkaWriterStates().size());
            for (KafkaWriterState kafkaWriterState : state.getKafkaWriterStates()) {
                writeKafkaWriterState(kafkaWriterState, out);
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public DynamicKafkaSinkWriterState deserialize(int version, byte[] serialized)
            throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            String routeId = in.readUTF();
            String kafkaClusterId = in.readUTF();
            String topic = in.readUTF();
            String transactionalIdPrefix = in.readUTF();
            Properties properties = DynamicKafkaSinkSerializationUtils.readProperties(in);
            int stateSize = in.readInt();
            List<KafkaWriterState> writerStates = new ArrayList<>(stateSize);
            for (int i = 0; i < stateSize; i++) {
                writerStates.add(readKafkaWriterState(in));
            }
            return new DynamicKafkaSinkWriterState(
                    routeId,
                    kafkaClusterId,
                    topic,
                    transactionalIdPrefix,
                    properties,
                    writerStates);
        }
    }

    private static void writeKafkaWriterState(KafkaWriterState state, DataOutputStream out)
            throws IOException {
        out.writeUTF(state.getTransactionalIdPrefix());
        out.writeInt(state.getOwnedSubtaskId());
        out.writeInt(state.getTotalNumberOfOwnedSubtasks());
        out.writeInt(state.getTransactionOwnership().ordinal());
        out.writeInt(state.getPrecommittedTransactionalIds().size());
        for (CheckpointTransaction transaction : state.getPrecommittedTransactionalIds()) {
            out.writeUTF(transaction.getTransactionalId());
            out.writeLong(transaction.getCheckpointId());
        }
    }

    private static KafkaWriterState readKafkaWriterState(DataInputStream in) throws IOException {
        String transactionalIdPrefix = in.readUTF();
        int ownedSubtaskId = in.readInt();
        int totalNumberOfOwnedSubtasks = in.readInt();
        TransactionOwnership transactionOwnership = TransactionOwnership.values()[in.readInt()];
        int precommittedSize = in.readInt();
        List<CheckpointTransaction> precommittedTransactions = new ArrayList<>(precommittedSize);
        for (int i = 0; i < precommittedSize; i++) {
            precommittedTransactions.add(new CheckpointTransaction(in.readUTF(), in.readLong()));
        }
        return new KafkaWriterState(
                transactionalIdPrefix,
                ownedSubtaskId,
                totalNumberOfOwnedSubtasks,
                transactionOwnership,
                precommittedTransactions);
    }
}
