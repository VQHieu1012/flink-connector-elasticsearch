/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for converting Flink internal rows into Elasticsearch document values. */
class RowDataToMapConverterTest {

    @Test
    void convertsNestedRowsAndArraysToJsonNativeStructures() {
        DataType geometryType =
                DataTypes.ROW(
                        DataTypes.FIELD("wkb", DataTypes.STRING()),
                        DataTypes.FIELD("srid", DataTypes.INT()));
        DataType physicalType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("geometry", geometryType),
                        DataTypes.FIELD("geometry_history", DataTypes.ARRAY(geometryType)));

        GenericRowData geometry =
                GenericRowData.of(StringData.fromString("AQEAAAA1XrpJDHZmwGq8dJMYbFbA"), null);
        GenericRowData historicalGeometry =
                GenericRowData.of(StringData.fromString("AQIAAAAB"), 4326);
        GenericRowData input =
                GenericRowData.of(
                        1L, geometry, new GenericArrayData(new Object[] {historicalGeometry}));

        Map<String, Object> result = new RowDataToMapConverter(physicalType).toMap(input);

        Map<?, ?> geometryResult = (Map<?, ?>) result.get("geometry");
        assertThat(geometryResult.get("wkb")).isEqualTo("AQEAAAA1XrpJDHZmwGq8dJMYbFbA");
        assertThat(geometryResult.get("srid")).isNull();
        assertThat(result.get("geometry_history"))
                .isEqualTo(List.of(Map.of("wkb", "AQIAAAAB", "srid", 4326)));
    }

    @Test
    void exposesTemporalValuesAsJavaTimeTypesForIsoSerialization() {
        LocalDate date = LocalDate.parse("2026-08-10");
        LocalTime time = LocalTime.parse("09:30:00.123");
        LocalDateTime timestamp = LocalDateTime.parse("2026-08-10T09:30:00.123456");
        Instant timestampLtz = Instant.parse("2026-08-10T09:30:00.123456Z");
        DataType physicalType =
                DataTypes.ROW(
                        DataTypes.FIELD("date", DataTypes.DATE()),
                        DataTypes.FIELD("time", DataTypes.TIME(3)),
                        DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP(6)),
                        DataTypes.FIELD("timestamp_ltz", DataTypes.TIMESTAMP_LTZ(6)));
        GenericRowData input =
                GenericRowData.of(
                        (int) date.toEpochDay(),
                        (int) (time.toNanoOfDay() / 1_000_000),
                        TimestampData.fromLocalDateTime(timestamp),
                        TimestampData.fromInstant(timestampLtz));

        Map<String, Object> result = new RowDataToMapConverter(physicalType).toMap(input);

        assertThat(result)
                .containsEntry("date", date)
                .containsEntry("time", time)
                .containsEntry("timestamp", timestamp)
                .containsEntry("timestamp_ltz", timestampLtz);
    }
}
