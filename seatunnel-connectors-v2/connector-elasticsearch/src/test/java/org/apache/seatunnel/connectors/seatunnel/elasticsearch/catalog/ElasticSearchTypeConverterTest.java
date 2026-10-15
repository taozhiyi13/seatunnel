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

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.GeometryKind;
import org.apache.seatunnel.api.table.type.GeometryType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ElasticSearchTypeConverterTest {

    private static String reconvertNativeType(SeaTunnelDataType<?> dataType) {
        BasicTypeDefine<EsType> typeDefine =
                ElasticSearchTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.of("col", dataType, (Long) null, true, null, null));
        Assertions.assertEquals(typeDefine.getColumnType(), typeDefine.getNativeType().getType());
        return typeDefine.getNativeType().getType();
    }

    @Test
    public void testReconvertPointToGeoPoint() {
        Assertions.assertEquals(
                EsType.GEO_POINT, reconvertNativeType(new GeometryType(GeometryKind.POINT)));
    }

    @Test
    public void testReconvertOtherGeometryKindsToGeoShape() {
        for (GeometryKind kind : GeometryKind.values()) {
            if (kind == GeometryKind.POINT) {
                continue;
            }
            Assertions.assertEquals(
                    EsType.GEO_SHAPE, reconvertNativeType(new GeometryType(kind)), "kind " + kind);
        }
    }

    @Test
    public void testReconvertStringUnchanged() {
        Assertions.assertEquals(EsType.TEXT, reconvertNativeType(BasicType.STRING_TYPE));
    }
}
