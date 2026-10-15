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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.type.Geometry;
import org.apache.seatunnel.api.table.type.GeometryKind;
import org.apache.seatunnel.api.table.type.GeometryType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GeometryJsonConverterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // WKB samples from the experiment database: SUBSTR(HEX(col), 9) of SRID 4326 values.
    // Expected GeoJSON is MySQL ST_AsGeoJSON(col), except that an empty collection becomes null.
    static final String WKB_P = "01010000000000000000605E403333333333333F40";
    static final String WKB_G = "01010000009A99999999195D403333333333F34340";
    static final String WKB_LS =
            "0102000000020000000000000000605E403333333333333F406666666666665E40CDCCCCCCCC4C3F40";
    static final String WKB_POLY =
            "010300000001000000050000009A99999999595E403333333333333F400000000000605E40"
                    + "3333333333333F400000000000605E40CDCCCCCCCC4C3F409A99999999595E40CDCCCCCC"
                    + "CC4C3F409A99999999595E403333333333333F40";
    static final String WKB_MP =
            "01040000000200000001010000000000000000605E403333333333333F4001010000006666666666665E40"
                    + "CDCCCCCCCC4C3F40";
    static final String WKB_MLS =
            "0105000000020000000102000000020000000000000000605E403333333333333F406666666666665E40"
                    + "CDCCCCCCCC4C3F40010200000002000000CDCCCCCCCC6C5E406666666666663F403333333333"
                    + "735E400000000000803F40";
    static final String WKB_MPOLY =
            "010600000001000000010300000001000000050000009A99999999595E403333333333333F40000000"
                    + "0000605E403333333333333F400000000000605E40CDCCCCCCCC4C3F409A99999999595E40CD"
                    + "CCCCCCCC4C3F409A99999999595E403333333333333F40";
    static final String WKB_GC =
            "01070000000200000001010000000000000000605E403333333333333F40010200000002000000000000"
                    + "0000605E403333333333333F406666666666665E40CDCCCCCCCC4C3F40";
    static final String WKB_GC_EMPTY = "010700000000000000";

    static final String POLY_GEOJSON =
            "{\"type\":\"Polygon\",\"coordinates\":[[[121.4,31.2],[121.5,31.2],[121.5,31.3],"
                    + "[121.4,31.3],[121.4,31.2]]]}";

    private static final Object[][] SAMPLES = {
        {GeometryKind.POINT, WKB_P, "{\"lon\":121.5,\"lat\":31.2}"},
        {GeometryKind.GEOMETRY, WKB_G, "{\"type\":\"Point\",\"coordinates\":[116.4,39.9]}"},
        {
            GeometryKind.LINESTRING,
            WKB_LS,
            "{\"type\":\"LineString\",\"coordinates\":[[121.5,31.2],[121.6,31.3]]}"
        },
        {GeometryKind.POLYGON, WKB_POLY, POLY_GEOJSON},
        {
            GeometryKind.MULTIPOINT,
            WKB_MP,
            "{\"type\":\"MultiPoint\",\"coordinates\":[[121.5,31.2],[121.6,31.3]]}"
        },
        {
            GeometryKind.MULTILINESTRING,
            WKB_MLS,
            "{\"type\":\"MultiLineString\",\"coordinates\":[[[121.5,31.2],[121.6,31.3]],"
                    + "[[121.7,31.4],[121.8,31.5]]]}"
        },
        {
            GeometryKind.MULTIPOLYGON,
            WKB_MPOLY,
            "{\"type\":\"MultiPolygon\",\"coordinates\":[[[[121.4,31.2],[121.5,31.2],[121.5,31.3],"
                    + "[121.4,31.3],[121.4,31.2]]]]}"
        },
        {
            GeometryKind.GEOMETRYCOLLECTION,
            WKB_GC,
            "{\"type\":\"GeometryCollection\",\"geometries\":["
                    + "{\"type\":\"Point\",\"coordinates\":[121.5,31.2]},"
                    + "{\"type\":\"LineString\",\"coordinates\":[[121.5,31.2],[121.6,31.3]]}]}"
        },
        {GeometryKind.GEOMETRYCOLLECTION, WKB_GC_EMPTY, null},
    };

    static byte[] hexToBytes(String hex) {
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    private static Object convert(GeometryKind kind, String wkbHex) {
        return GeometryJsonConverter.toOpenSearchValue(
                new GeometryType(kind), new Geometry(4326, hexToBytes(wkbHex)));
    }

    @Test
    public void testExperimentSamples() throws Exception {
        for (Object[] sample : SAMPLES) {
            GeometryKind kind = (GeometryKind) sample[0];
            String expected = (String) sample[2];
            Object actual = convert(kind, (String) sample[1]);
            if (expected == null) {
                Assertions.assertNull(actual, "kind " + kind);
            } else {
                Assertions.assertEquals(
                        MAPPER.readTree(expected), MAPPER.valueToTree(actual), "kind " + kind);
            }
        }
    }

    @Test
    public void testBigEndianPoint() throws Exception {
        Object actual = convert(GeometryKind.POINT, "0000000001405E600000000000403F333333333333");
        Assertions.assertEquals(
                MAPPER.readTree("{\"lon\":121.5,\"lat\":31.2}"), MAPPER.valueToTree(actual));
    }

    @Test
    public void testRejectNonWgs84Srid() {
        for (long srid : new long[] {0L, 3857L, Integer.MAX_VALUE, 0x8000_0000L, 0xFFFF_FFFFL}) {
            IllegalArgumentException e =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    GeometryJsonConverter.toOpenSearchValue(
                                            new GeometryType(GeometryKind.POINT),
                                            new Geometry(srid, hexToBytes(WKB_P))));
            Assertions.assertTrue(e.getMessage().contains("SRID " + srid), e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("4326"), e.getMessage());
        }
    }

    @Test
    public void testPointColumnRejectsLineString() {
        IllegalArgumentException e =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> convert(GeometryKind.POINT, WKB_LS));
        Assertions.assertTrue(e.getMessage().contains("LineString"), e.getMessage());
    }

    @Test
    public void testRejectZDimension() {
        // little-endian header, type 1001 (POINT Z), then x, y, z
        String pointZ = "01E9030000" + "0000000000605E40" + "3333333333333F40" + "0000000000000000";
        IllegalArgumentException e =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> convert(GeometryKind.GEOMETRY, pointZ));
        Assertions.assertTrue(e.getMessage().contains("1001"), e.getMessage());
    }

    @Test
    public void testRejectTrailingBytes() {
        IllegalArgumentException e =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> convert(GeometryKind.POINT, WKB_P + "00"));
        Assertions.assertTrue(e.getMessage().contains("trailing"), e.getMessage());
    }
}
