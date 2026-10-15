/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.Geometry;
import org.apache.seatunnel.api.table.type.GeometryKind;
import org.apache.seatunnel.api.table.type.GeometryType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalTime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MysqlJdbcRowConverterTest {

    @Test
    public void testTimeReadAndWritePreserveMicroseconds() throws Exception {
        LocalTime expected = LocalTime.parse("12:34:56.123456");
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("time_col")
                                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                                        .build())
                        .build();
        MysqlJdbcRowConverter converter = new MysqlJdbcRowConverter();
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getObject(1, LocalTime.class)).thenReturn(expected);

        SeaTunnelRow result = converter.toInternal(resultSet, tableSchema);
        assertEquals(expected, result.getField(0));

        PreparedStatement statement = mock(PreparedStatement.class);
        converter.toExternal(tableSchema, new SeaTunnelRow(new Object[] {expected}), statement);
        ArgumentCaptor<Timestamp> timestamp = ArgumentCaptor.forClass(Timestamp.class);
        verify(statement).setTimestamp(org.mockito.ArgumentMatchers.eq(1), timestamp.capture());
        assertEquals(expected.getNano(), timestamp.getValue().getNanos());
    }

    @Test
    public void testGeometryReadsLittleEndianSridAndWkb() throws Exception {
        byte[] wkb = hexToBytes("01010000000000000000605E403333333333333F40");
        byte[] raw = new byte[Integer.BYTES + wkb.length];
        raw[0] = (byte) 0xE6;
        raw[1] = (byte) 0x10;
        System.arraycopy(wkb, 0, raw, Integer.BYTES, wkb.length);

        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("location")
                                        .dataType(new GeometryType(GeometryKind.POINT))
                                        .build())
                        .build();
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getBytes(1)).thenReturn(raw);

        SeaTunnelRow result = new MysqlJdbcRowConverter().toInternal(resultSet, tableSchema);
        Geometry geometry = (Geometry) result.getField(0);
        assertEquals(4326L, geometry.getSrid());
        assertArrayEquals(wkb, geometry.getWkb());
    }

    @Test
    public void testNullGeometry() throws Exception {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("location")
                                        .dataType(new GeometryType(GeometryKind.POINT))
                                        .build())
                        .build();
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getBytes(1)).thenReturn(null);

        SeaTunnelRow result = new MysqlJdbcRowConverter().toInternal(resultSet, tableSchema);
        assertNull(result.getField(0));
    }

    @Test
    public void testGeometryShorterThanSridPrefixFails() throws Exception {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("location")
                                        .dataType(new GeometryType(GeometryKind.GEOMETRY))
                                        .build())
                        .build();
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getBytes(1)).thenReturn(new byte[] {1, 2});

        assertThrows(
                SQLException.class,
                () -> new MysqlJdbcRowConverter().toInternal(resultSet, tableSchema));
    }

    private static byte[] hexToBytes(String hex) {
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }
}
