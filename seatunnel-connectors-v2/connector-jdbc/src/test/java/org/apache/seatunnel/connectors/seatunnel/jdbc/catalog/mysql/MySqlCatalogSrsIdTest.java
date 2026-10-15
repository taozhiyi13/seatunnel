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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.GeometryKind;
import org.apache.seatunnel.api.table.type.GeometryType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MySqlCatalogSrsIdTest {

    private static ResultSet resultSet(String[] labels, Object srsId) throws SQLException {
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(metaData.getColumnCount()).thenReturn(labels.length);
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getMetaData()).thenReturn(metaData);
        for (int i = 0; i < labels.length; i++) {
            when(metaData.getColumnLabel(i + 1)).thenReturn(labels[i]);
            if ("SRS_ID".equals(labels[i])) {
                when(resultSet.getObject(i + 1)).thenReturn(srsId);
            }
        }
        return resultSet;
    }

    private static Column pointColumn() {
        return PhysicalColumn.builder()
                .name("p")
                .dataType(new GeometryType(GeometryKind.POINT))
                .nullable(true)
                .comment("location")
                .sourceType("point")
                .build();
    }

    @Test
    void testMissingSrsIdColumnIsUndeclared() throws SQLException {
        ResultSet rs = resultSet(new String[] {"COLUMN_NAME", "DATA_TYPE"}, null);
        Assertions.assertNull(MySqlCatalog.readSrsId(rs));
    }

    @Test
    void testNullSrsIdIsUndeclared() throws SQLException {
        ResultSet rs = resultSet(new String[] {"COLUMN_NAME", "SRS_ID"}, null);
        Assertions.assertNull(MySqlCatalog.readSrsId(rs));
    }

    @Test
    void testDeclaredSrsIdIsRead() throws SQLException {
        Assertions.assertEquals(0L, MySqlCatalog.readSrsId(resultSet(new String[] {"SRS_ID"}, 0L)));
        Assertions.assertEquals(
                4326L, MySqlCatalog.readSrsId(resultSet(new String[] {"SRS_ID"}, 4326L)));
        Assertions.assertEquals(
                3857L, MySqlCatalog.readSrsId(resultSet(new String[] {"SRS_ID"}, 3857)));
        Assertions.assertEquals(
                4294967295L,
                MySqlCatalog.readSrsId(resultSet(new String[] {"SRS_ID"}, 4294967295L)));
    }

    @Test
    void testUndeclaredSrsIdLeavesColumnUnchanged() {
        Column column = pointColumn();
        Assertions.assertSame(column, MySqlCatalog.withSrsId(column, null));
    }

    @Test
    void testDeclaredSrsIdIsStoredInOptions() {
        for (long srsId : new long[] {0L, 3857L, 4326L}) {
            Column column = MySqlCatalog.withSrsId(pointColumn(), srsId);
            Assertions.assertEquals(srsId, column.getOptions().get(GeometryType.SRID_OPTION));
            Assertions.assertEquals(new GeometryType(GeometryKind.POINT), column.getDataType());
            Assertions.assertEquals("p", column.getName());
            Assertions.assertEquals("point", column.getSourceType());
            Assertions.assertEquals("location", column.getComment());
            Assertions.assertTrue(column.isNullable());
        }
    }

    @Test
    void testSrsIdIgnoredForNonGeometryColumn() {
        Column column = PhysicalColumn.builder().name("id").dataType(BasicType.LONG_TYPE).build();
        Assertions.assertSame(column, MySqlCatalog.withSrsId(column, 4326L));
    }
}
