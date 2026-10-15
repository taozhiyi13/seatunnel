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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.GeometryKind;
import org.apache.seatunnel.api.table.type.GeometryType;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class ElasticsearchSinkFactoryGeometrySridTest {

    private static Column geometry(String name, GeometryKind kind, Object srid) {
        Map<String, Object> options = new HashMap<>();
        if (srid != null) {
            options.put(GeometryType.SRID_OPTION, srid);
        }
        return PhysicalColumn.builder()
                .name(name)
                .dataType(new GeometryType(kind))
                .nullable(true)
                .options(options)
                .build();
    }

    private static CatalogTable table(Column... columns) {
        TableSchema.Builder schema = TableSchema.builder();
        schema.column(PhysicalColumn.of("id", BasicType.LONG_TYPE, (Long) null, false, null, null));
        for (Column column : columns) {
            schema.column(column);
        }
        return CatalogTable.of(
                TableIdentifier.of("MySQL-CDC", "spatial", "geo"),
                schema.build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    private static void assertPasses(CatalogTable table) {
        Assertions.assertDoesNotThrow(
                () -> ElasticsearchSinkFactory.validateDeclaredGeometrySrids(table));
    }

    private static SeaTunnelException assertRejects(CatalogTable table) {
        return Assertions.assertThrows(
                SeaTunnelException.class,
                () -> ElasticsearchSinkFactory.validateDeclaredGeometrySrids(table));
    }

    @Test
    void testUndeclaredSridPasses() {
        assertPasses(table(geometry("p", GeometryKind.POINT, null)));
    }

    @Test
    void testColumnWithoutOptionsPasses() {
        assertPasses(
                table(
                        PhysicalColumn.builder()
                                .name("p")
                                .dataType(new GeometryType(GeometryKind.POINT))
                                .build()));
    }

    @Test
    void testWgs84Passes() {
        assertPasses(table(geometry("p", GeometryKind.POINT, 4326L)));
        assertPasses(table(geometry("p", GeometryKind.POINT, 4326)));
    }

    @Test
    void testSridZeroIsRejected() {
        SeaTunnelException e = assertRejects(table(geometry("g", GeometryKind.GEOMETRY, 0L)));
        Assertions.assertTrue(e.getMessage().contains("Column [g]"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("spatial.geo"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("SRID 0"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("4326"), e.getMessage());
    }

    @Test
    void testOtherSridIsRejected() {
        SeaTunnelException e = assertRejects(table(geometry("p", GeometryKind.POINT, 3857L)));
        Assertions.assertTrue(e.getMessage().contains("SRID 3857"), e.getMessage());
    }

    @Test
    void testAnyIncompatibleColumnRejectsTable() {
        SeaTunnelException e =
                assertRejects(
                        table(
                                geometry("p", GeometryKind.POINT, 4326L),
                                geometry("area", GeometryKind.POLYGON, 0L)));
        Assertions.assertTrue(e.getMessage().contains("Column [area]"), e.getMessage());
    }

    @Test
    void testTableWithoutGeometryPasses() {
        assertPasses(table());
    }

    private static TableSinkFactoryContext context(CatalogTable table) {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Collections.singletonList("http://127.0.0.1:1"));
        config.put("index", "geo");
        return new TableSinkFactoryContext(
                table,
                ReadonlyConfig.fromMap(config),
                ElasticsearchSinkFactoryGeometrySridTest.class.getClassLoader());
    }

    @Test
    void testSubmissionRejectsBeforeConnecting() {
        TableSinkFactoryContext context = context(table(geometry("g", GeometryKind.GEOMETRY, 0L)));
        ElasticsearchSinkFactory factory = new ElasticsearchSinkFactory();

        SeaTunnelException createSink =
                Assertions.assertThrows(
                        SeaTunnelException.class, () -> factory.createSink(context));
        Assertions.assertTrue(
                createSink.getMessage().contains("declares SRID 0"), createSink.getMessage());
    }
}
