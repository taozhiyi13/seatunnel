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

import org.apache.seatunnel.api.table.type.Geometry;
import org.apache.seatunnel.api.table.type.GeometryKind;
import org.apache.seatunnel.api.table.type.GeometryType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts a {@link Geometry} into a value that OpenSearch can parse: {@code {"lon": x, "lat": y}}
 * for {@code geo_point} columns, GeoJSON for {@code geo_shape} columns. WKB x is longitude and y is
 * latitude.
 */
public final class GeometryJsonConverter {

    /** OpenSearch geo fields index coordinates as WGS84. */
    public static final long OPENSEARCH_SRID = 4326L;

    private static final int WKB_POINT = 1;
    private static final int WKB_LINESTRING = 2;
    private static final int WKB_POLYGON = 3;
    private static final int WKB_MULTIPOINT = 4;
    private static final int WKB_MULTILINESTRING = 5;
    private static final int WKB_MULTIPOLYGON = 6;
    private static final int WKB_GEOMETRYCOLLECTION = 7;

    private static final String[] GEOJSON_TYPES = {
        null,
        "Point",
        "LineString",
        "Polygon",
        "MultiPoint",
        "MultiLineString",
        "MultiPolygon",
        "GeometryCollection"
    };

    private GeometryJsonConverter() {}

    /**
     * Returns {@code {"lon": x, "lat": y}} when the column kind is POINT, otherwise GeoJSON.
     * Returns null for an empty geometry collection, which OpenSearch does not accept.
     *
     * @throws IllegalArgumentException if the geometry does not use WGS84 (SRID 4326)
     */
    public static Object toOpenSearchValue(GeometryType type, Geometry geometry) {
        if (geometry.getSrid() != OPENSEARCH_SRID) {
            throw new IllegalArgumentException(
                    "Unsupported SRID "
                            + geometry.getSrid()
                            + ", OpenSearch geo fields only accept SRID "
                            + OPENSEARCH_SRID);
        }
        WkbReader reader = new WkbReader(geometry.getWkb());
        ParsedGeometry parsed = reader.readGeometry();
        reader.requireFullyConsumed();

        if (type.getKind() == GeometryKind.POINT) {
            if (parsed.wkbType != WKB_POINT) {
                throw new IllegalArgumentException(
                        "POINT column expects a WKB point, but got "
                                + GEOJSON_TYPES[parsed.wkbType]);
            }
            List<?> xy = (List<?>) parsed.coordinates;
            Map<String, Object> point = new LinkedHashMap<>();
            point.put("lon", xy.get(0));
            point.put("lat", xy.get(1));
            return point;
        }
        return toGeoJson(parsed);
    }

    private static Map<String, Object> toGeoJson(ParsedGeometry parsed) {
        Map<String, Object> json = new LinkedHashMap<>();
        if (parsed.wkbType == WKB_GEOMETRYCOLLECTION) {
            List<Map<String, Object>> geometries = new ArrayList<>();
            for (ParsedGeometry child : parsed.children) {
                Map<String, Object> childJson = toGeoJson(child);
                if (childJson != null) {
                    geometries.add(childJson);
                }
            }
            if (geometries.isEmpty()) {
                return null;
            }
            json.put("type", GEOJSON_TYPES[WKB_GEOMETRYCOLLECTION]);
            json.put("geometries", geometries);
            return json;
        }
        json.put("type", GEOJSON_TYPES[parsed.wkbType]);
        json.put("coordinates", parsed.coordinates);
        return json;
    }

    /** A parsed WKB geometry: GeoJSON-shaped coordinates, or children for a collection. */
    private static final class ParsedGeometry {
        private final int wkbType;
        private final Object coordinates;
        private final List<ParsedGeometry> children;

        private ParsedGeometry(int wkbType, Object coordinates, List<ParsedGeometry> children) {
            this.wkbType = wkbType;
            this.coordinates = coordinates;
            this.children = children;
        }
    }

    private static final class WkbReader {
        private final ByteBuffer buffer;

        private WkbReader(byte[] wkb) {
            this.buffer = ByteBuffer.wrap(wkb);
        }

        private ParsedGeometry readGeometry() {
            ByteOrder order = readByteOrder();
            int wkbType = readCount(order, "geometry type");
            if (wkbType < WKB_POINT || wkbType > WKB_GEOMETRYCOLLECTION) {
                throw new IllegalArgumentException(
                        "Unsupported WKB geometry type "
                                + wkbType
                                + ", only 2D types 1-7 are supported");
            }
            switch (wkbType) {
                case WKB_POINT:
                    return new ParsedGeometry(wkbType, readPoint(order), null);
                case WKB_LINESTRING:
                    return new ParsedGeometry(wkbType, readPoints(order), null);
                case WKB_POLYGON:
                    return new ParsedGeometry(wkbType, readRings(order), null);
                case WKB_MULTIPOINT:
                    return new ParsedGeometry(wkbType, readMembers(order, WKB_POINT), null);
                case WKB_MULTILINESTRING:
                    return new ParsedGeometry(wkbType, readMembers(order, WKB_LINESTRING), null);
                case WKB_MULTIPOLYGON:
                    return new ParsedGeometry(wkbType, readMembers(order, WKB_POLYGON), null);
                default:
                    int count = readCount(order, "geometry count");
                    List<ParsedGeometry> children = new ArrayList<>();
                    for (int i = 0; i < count; i++) {
                        children.add(readGeometry());
                    }
                    return new ParsedGeometry(wkbType, null, children);
            }
        }

        /** Reads the members of a MULTI* geometry, each a complete WKB of {@code memberType}. */
        private List<Object> readMembers(ByteOrder order, int memberType) {
            int count = readCount(order, "member count");
            List<Object> members = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                ParsedGeometry member = readGeometry();
                if (member.wkbType != memberType) {
                    throw new IllegalArgumentException(
                            "Invalid WKB: "
                                    + GEOJSON_TYPES[memberType]
                                    + " container holds a "
                                    + GEOJSON_TYPES[member.wkbType]);
                }
                members.add(member.coordinates);
            }
            return members;
        }

        private List<List<List<Double>>> readRings(ByteOrder order) {
            int ringCount = readCount(order, "ring count");
            List<List<List<Double>>> rings = new ArrayList<>();
            for (int i = 0; i < ringCount; i++) {
                rings.add(readPoints(order));
            }
            return rings;
        }

        private List<List<Double>> readPoints(ByteOrder order) {
            int pointCount = readCount(order, "point count");
            if ((long) pointCount * 16 > buffer.remaining()) {
                throw new IllegalArgumentException(
                        "Invalid WKB: point count " + pointCount + " exceeds remaining bytes");
            }
            List<List<Double>> points = new ArrayList<>(pointCount);
            for (int i = 0; i < pointCount; i++) {
                points.add(readPoint(order));
            }
            return points;
        }

        private List<Double> readPoint(ByteOrder order) {
            double x = readDouble(order);
            double y = readDouble(order);
            if (Double.isNaN(x) || Double.isNaN(y)) {
                throw new IllegalArgumentException("Invalid WKB: coordinate is NaN");
            }
            return Arrays.asList(x, y);
        }

        private ByteOrder readByteOrder() {
            require(1);
            byte flag = buffer.get();
            if (flag == 0) {
                return ByteOrder.BIG_ENDIAN;
            }
            if (flag == 1) {
                return ByteOrder.LITTLE_ENDIAN;
            }
            throw new IllegalArgumentException("Invalid WKB byte order flag " + flag);
        }

        /** Reads an unsigned 32-bit integer that must fit in a non-negative int. */
        private int readCount(ByteOrder order, String what) {
            require(4);
            long value = buffer.order(order).getInt() & 0xFFFFFFFFL;
            if (value > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Invalid WKB " + what + " " + value);
            }
            return (int) value;
        }

        private double readDouble(ByteOrder order) {
            require(8);
            return buffer.order(order).getDouble();
        }

        private void require(int length) {
            if (buffer.remaining() < length) {
                throw new IllegalArgumentException(
                        "Invalid WKB: unexpected end of data at offset " + buffer.position());
            }
        }

        private void requireFullyConsumed() {
            if (buffer.hasRemaining()) {
                throw new IllegalArgumentException(
                        "Invalid WKB: " + buffer.remaining() + " trailing bytes");
            }
        }
    }
}
