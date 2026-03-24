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

package org.apache.flink.dynamic.sink.util;

import java.util.LinkedHashMap;
import java.util.Map;

/** CLI config utility. */
public final class ConfigUtil {

    private ConfigUtil() {}

    public static Map<String, String> parseArgs(String[] args) {
        Map<String, String> parameters = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String key = args[i];
            if (!key.startsWith("--")) {
                throw new IllegalArgumentException("Unsupported argument: " + key);
            }
            String normalizedKey = key.substring(2);
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Missing value for argument --" + normalizedKey);
            }
            parameters.put(normalizedKey, args[++i]);
        }
        return parameters;
    }

    public static String getRequired(Map<String, String> parameters, String key) {
        String value = parameters.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Missing required argument --" + key);
        }
        return value;
    }
}
