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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.GeometryKind;
import org.apache.seatunnel.api.table.type.GeometryType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class ElasticSearchCatalogTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static CatalogTable table(String[] names, SeaTunnelDataType<?>[] types) {
        TableSchema.Builder schema = TableSchema.builder();
        for (int i = 0; i < names.length; i++) {
            schema.column(PhysicalColumn.of(names[i], types[i], (Long) null, true, null, null));
        }
        return CatalogTable.of(
                TableIdentifier.of("es", "", "idx"),
                schema.build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    @Test
    public void testNoGeometryColumnReturnsNull() {
        CatalogTable table =
                table(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE, BasicType.STRING_TYPE});
        Assertions.assertNull(ElasticSearchCatalog.buildGeometryMapping(table));
    }

    @Test
    public void testOnlyGeometryColumnsAreMapped() throws Exception {
        CatalogTable table =
                table(
                        new String[] {"id", "p", "area", "g"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.LONG_TYPE,
                            new GeometryType(GeometryKind.POINT),
                            new GeometryType(GeometryKind.POLYGON),
                            new GeometryType(GeometryKind.GEOMETRY)
                        });
        String mapping = ElasticSearchCatalog.buildGeometryMapping(table);

        JsonNode expected =
                MAPPER.readTree(
                        "{\"mappings\":{\"properties\":{"
                                + "\"p\":{\"type\":\"geo_point\"},"
                                + "\"area\":{\"type\":\"geo_shape\"},"
                                + "\"g\":{\"type\":\"geo_shape\"}}}}");
        Assertions.assertEquals(expected, MAPPER.readTree(mapping));
    }

    @Test
    public void testNullTableReturnsNull() {
        Assertions.assertNull(ElasticSearchCatalog.buildGeometryMapping(null));
    }
}
