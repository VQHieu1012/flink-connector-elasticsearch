/*
 *
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Tool class used to convert from {@link RowData} to {@link Map}. * */
@Internal
public class RowDataToMapConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DataType physicalDataType;

    public RowDataToMapConverter(DataType physicalDataType) {
        this.physicalDataType = physicalDataType;
    }

    public Map<String, Object> toMap(RowData rowData) {
        List<DataTypes.Field> fields = DataType.getFields(physicalDataType);

        Map<String, Object> map = new LinkedHashMap<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            DataTypes.Field field = fields.get(i);
            RowData.FieldGetter fieldGetter =
                    RowData.createFieldGetter(field.getDataType().getLogicalType(), i);

            String key = field.getName();
            Object externalValue =
                    DataStructureConverters.getConverter(field.getDataType())
                            .toExternalOrNull(fieldGetter.getFieldOrNull(rowData));

            map.put(key, normalizeExternalValue(field.getDataType(), externalValue));
        }
        return map;
    }

    /** Convert structured external values into JSON-native maps and lists. */
    private static Object normalizeExternalValue(DataType dataType, Object value) {
        if (value == null) {
            return null;
        }

        switch (dataType.getLogicalType().getTypeRoot()) {
            case ROW:
                if (!(value instanceof Row)) {
                    throw unexpectedExternalValue(dataType, value, Row.class);
                }
                Row row = (Row) value;
                List<DataTypes.Field> rowFields = DataType.getFields(dataType);
                Map<String, Object> rowMap = new LinkedHashMap<>(rowFields.size());
                for (int i = 0; i < rowFields.size(); i++) {
                    DataTypes.Field field = rowFields.get(i);
                    rowMap.put(
                            field.getName(),
                            normalizeExternalValue(field.getDataType(), row.getField(i)));
                }
                return rowMap;
            case ARRAY:
                DataType elementType = dataType.getChildren().get(0);
                int length = java.lang.reflect.Array.getLength(value);
                List<Object> elements = new ArrayList<>(length);
                for (int i = 0; i < length; i++) {
                    elements.add(
                            normalizeExternalValue(
                                    elementType, java.lang.reflect.Array.get(value, i)));
                }
                return elements;
            case MAP:
                if (!(value instanceof Map)) {
                    throw unexpectedExternalValue(dataType, value, Map.class);
                }
                DataType keyType = dataType.getChildren().get(0);
                DataType valueType = dataType.getChildren().get(1);
                Map<Object, Object> normalizedMap = new LinkedHashMap<>();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                    normalizedMap.put(
                            normalizeExternalValue(keyType, entry.getKey()),
                            normalizeExternalValue(valueType, entry.getValue()));
                }
                return normalizedMap;
            default:
                return value;
        }
    }

    private static IllegalArgumentException unexpectedExternalValue(
            DataType dataType, Object value, Class<?> expectedType) {
        return new IllegalArgumentException(
                "Expected external "
                        + expectedType.getSimpleName()
                        + " value for "
                        + dataType
                        + ", got "
                        + value.getClass().getName());
    }
}
