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

package org.apache.seatunnel.api.table.type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GeometryTest {

    static final String POINT_WKB_HEX = "01010000000000000000605E403333333333333F40";

    static byte[] hexToBytes(String hex) {
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    @Test
    void testPreservesUnsigned32BitSrid() {
        byte[] wkb = hexToBytes(POINT_WKB_HEX);
        for (long srid :
                new long[] {
                    Geometry.MIN_SRID,
                    3857L,
                    4326L,
                    Integer.MAX_VALUE,
                    0x8000_0000L,
                    Geometry.MAX_SRID
                }) {
            Geometry geometry = new Geometry(srid, wkb);
            Assertions.assertEquals(srid, geometry.getSrid());
            Assertions.assertArrayEquals(wkb, geometry.getWkb());
        }
        Assertions.assertNotEquals(new Geometry(0, wkb), new Geometry(4326, wkb));
    }

    @Test
    void testRejectSridOutsideUnsigned32BitRange() {
        byte[] wkb = hexToBytes(POINT_WKB_HEX);
        for (long srid : new long[] {-1L, Geometry.MAX_SRID + 1L}) {
            IllegalArgumentException e =
                    Assertions.assertThrows(
                            IllegalArgumentException.class, () -> new Geometry(srid, wkb));
            Assertions.assertTrue(e.getMessage().contains(Long.toString(srid)), e.getMessage());
        }
    }

    @Test
    void testRejectNullWkb() {
        Assertions.assertThrows(NullPointerException.class, () -> new Geometry(4326, null));
    }

    @Test
    void testEqualsHashCodeAndToString() {
        Geometry g1 = new Geometry(4326, hexToBytes(POINT_WKB_HEX));
        Geometry g2 = new Geometry(4326, hexToBytes(POINT_WKB_HEX));
        Geometry g3 = new Geometry(4326, hexToBytes("010700000000000000"));

        Assertions.assertEquals(g1, g2);
        Assertions.assertEquals(g1.hashCode(), g2.hashCode());
        Assertions.assertNotEquals(g1, g3);
        Assertions.assertEquals("Geometry{srid=4326, wkb=" + POINT_WKB_HEX + "}", g1.toString());
    }

    @Test
    void testGeometryType() {
        GeometryType point = new GeometryType(GeometryKind.POINT);

        Assertions.assertEquals(point, new GeometryType(GeometryKind.POINT));
        Assertions.assertEquals(point.hashCode(), new GeometryType(GeometryKind.POINT).hashCode());
        Assertions.assertNotEquals(point, new GeometryType(GeometryKind.POLYGON));
        Assertions.assertEquals(SqlType.GEOMETRY, point.getSqlType());
        Assertions.assertEquals(Geometry.class, point.getTypeClass());
        Assertions.assertEquals(GeometryKind.POINT, point.getKind());
        Assertions.assertThrows(NullPointerException.class, () -> new GeometryType(null));
    }
}
