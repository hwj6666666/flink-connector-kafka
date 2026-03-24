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

import org.apache.flink.util.InstantiationUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Properties;

/** Serialization helpers for dynamic Kafka sink state. */
public final class DynamicKafkaSinkSerializationUtils {

    private DynamicKafkaSinkSerializationUtils() {}

    public static void writeProperties(Properties properties, DataOutputStream out)
            throws IOException {
        out.writeInt(properties.stringPropertyNames().size());
        for (String key : properties.stringPropertyNames()) {
            out.writeUTF(key);
            out.writeUTF(properties.getProperty(key));
        }
    }

    public static Properties readProperties(DataInputStream in) throws IOException {
        int size = in.readInt();
        Properties properties = new Properties();
        for (int i = 0; i < size; i++) {
            properties.setProperty(in.readUTF(), in.readUTF());
        }
        return properties;
    }

    public static <T> void writeSerializableObject(T object, DataOutputStream out)
            throws IOException {
        byte[] serialized = InstantiationUtil.serializeObject(object);
        out.writeInt(serialized.length);
        out.write(serialized);
    }

    public static <T> T readSerializableObject(DataInputStream in, Class<T> clazz)
            throws IOException {
        int size = in.readInt();
        byte[] serialized = new byte[size];
        in.readFully(serialized);
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (classLoader == null) {
                classLoader = DynamicKafkaSinkSerializationUtils.class.getClassLoader();
            }
            return InstantiationUtil.deserializeObject(serialized, classLoader);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize " + clazz.getSimpleName(), e);
        }
    }
}
