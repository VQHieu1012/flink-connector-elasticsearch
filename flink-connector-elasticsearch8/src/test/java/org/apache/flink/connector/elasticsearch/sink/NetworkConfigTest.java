/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.elasticsearch.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the JSON serialization contract used by Elasticsearch clients. */
class NetworkConfigTest {

    @Test
    void testTemporalValuesAreSerializedAsIso8601Strings() throws Exception {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("date", LocalDate.parse("2026-08-10"));
        document.put("time", LocalTime.parse("09:30:00.123456"));
        document.put("timestamp", LocalDateTime.parse("2026-08-10T09:30:00.123456"));
        document.put("timestamp_ltz", Instant.parse("2026-08-10T09:30:00.123456Z"));
        document.put("nested", List.of(Instant.parse("2026-08-11T10:00:00Z")));

        ObjectMapper mapper = NetworkConfig.createObjectMapper();
        JsonNode json = mapper.readTree(mapper.writeValueAsBytes(document));

        assertThat(json.get("date").textValue()).isEqualTo("2026-08-10");
        assertThat(json.get("time").textValue()).isEqualTo("09:30:00.123456");
        assertThat(json.get("timestamp").textValue()).isEqualTo("2026-08-10T09:30:00.123456");
        assertThat(json.get("timestamp_ltz").textValue()).isEqualTo("2026-08-10T09:30:00.123456Z");
        assertThat(json.get("nested").get(0).textValue()).isEqualTo("2026-08-11T10:00:00Z");
    }
}
