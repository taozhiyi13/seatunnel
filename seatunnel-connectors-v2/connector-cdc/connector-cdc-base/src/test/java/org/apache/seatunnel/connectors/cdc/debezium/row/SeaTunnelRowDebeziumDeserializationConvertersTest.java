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

package org.apache.seatunnel.connectors.cdc.debezium.row;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.GeometryKind;
import org.apache.seatunnel.api.table.type.GeometryType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverter;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverterFactory;
import org.apache.seatunnel.connectors.cdc.debezium.MetadataConverter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class SeaTunnelRowDebeziumDeserializationConvertersTest {

    @Test
    void testDefaultValueNotUsed() throws Exception {
        SeaTunnelRowDebeziumDeserializationConverters converters =
                new SeaTunnelRowDebeziumDeserializationConverters(
                        new SeaTunnelRowType(
                                new String[] {"id", "name"},
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE
                                }),
                        new MetadataConverter[] {},
                        ZoneId.systemDefault(),
                        DebeziumDeserializationConverterFactory.DEFAULT);
        Schema schema =
                SchemaBuilder.struct()
                        .field("id", SchemaBuilder.int32().build())
                        .field("name", SchemaBuilder.string().defaultValue("UL"))
                        .build();
        Struct value = new Struct(schema);
        // the value of `name` is null, so do not put value for it
        value.put("id", 1);
        SourceRecord record =
                new SourceRecord(
                        new HashMap<>(),
                        new HashMap<>(),
                        "topicName",
                        null,
                        SchemaBuilder.int32().build(),
                        1,
                        schema,
                        value,
                        null,
                        new ArrayList<>());

        SeaTunnelRow row = converters.convert(record, value, schema);
        Assertions.assertEquals(row.getField(0), 1);
        Assertions.assertNull(row.getField(1));
    }

    @Test
    void testArrayConverter() throws Exception {
        DebeziumDeserializationConverter converter;
        // bool array converter
        converter =
                SeaTunnelRowDebeziumDeserializationConverters.createArrayConverter(
                        ArrayType.BOOLEAN_ARRAY_TYPE);
        Boolean[] booleans = new Boolean[] {false, true};
        Assertions.assertTrue(
                Arrays.equals(
                        booleans, (Boolean[]) (converter.convert(Arrays.asList(booleans), null))));
        // smallInt array converter
        converter =
                SeaTunnelRowDebeziumDeserializationConverters.createArrayConverter(
                        ArrayType.SHORT_ARRAY_TYPE);
        Short[] shorts = new Short[] {(short) 1, (short) 2};
        Assertions.assertTrue(
                Arrays.equals(shorts, (Short[]) (converter.convert(Arrays.asList(shorts), null))));
        // int array converter
        converter =
                SeaTunnelRowDebeziumDeserializationConverters.createArrayConverter(
                        ArrayType.INT_ARRAY_TYPE);
        Integer[] ints = new Integer[] {1, 2};
        Assertions.assertTrue(
                Arrays.equals(ints, (Integer[]) (converter.convert(Arrays.asList(ints), null))));
        // long array converter
        converter =
                SeaTunnelRowDebeziumDeserializationConverters.createArrayConverter(
                        ArrayType.LONG_ARRAY_TYPE);
        Long[] longs = new Long[] {1L, 2L};
        Assertions.assertTrue(
                Arrays.equals(longs, (Long[]) (converter.convert(Arrays.asList(longs), null))));
        // float array converter
        converter =
                SeaTunnelRowDebeziumDeserializationConverters.createArrayConverter(
                        ArrayType.FLOAT_ARRAY_TYPE);
        Float[] floats = new Float[] {1.0f, 2.0f};
        Assertions.assertTrue(
                Arrays.equals(floats, (Float[]) (converter.convert(Arrays.asList(floats), null))));
        // double array converter
        converter =
                SeaTunnelRowDebeziumDeserializationConverters.createArrayConverter(
                        ArrayType.DOUBLE_ARRAY_TYPE);
        Double[] doubles = new Double[] {1.0, 2.0};
        Assertions.assertTrue(
                Arrays.equals(
                        doubles, (Double[]) (converter.convert(Arrays.asList(doubles), null))));
    }

    @Test
    void testGeometryStringConversion() throws Exception {
        SeaTunnelRowDebeziumDeserializationConverters converters =
                new SeaTunnelRowDebeziumDeserializationConverters(
                        new SeaTunnelRowType(
                                new String[] {"geo"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}),
                        new MetadataConverter[] {},
                        ZoneId.systemDefault(),
                        DebeziumDeserializationConverterFactory.DEFAULT);

        byte[] wkb = new byte[] {0x01, 0x02, (byte) 0xFF};
        Schema geometrySchema = Geometry.builder().optional().build();
        Schema recordSchema = SchemaBuilder.struct().field("geo", geometrySchema).build();

        Struct geometryValue = Geometry.createValue(geometrySchema, wkb, 4549);
        Struct recordValue = new Struct(recordSchema);
        recordValue.put("geo", geometryValue);

        SourceRecord record =
                new SourceRecord(
                        new HashMap<>(),
                        new HashMap<>(),
                        "topicName",
                        null,
                        SchemaBuilder.int32().build(),
                        1,
                        recordSchema,
                        recordValue,
                        null,
                        new ArrayList<>());

        SeaTunnelRow row = converters.convert(record, recordValue, recordSchema);
        Object fieldValue = row.getField(0);
        Assertions.assertTrue(fieldValue instanceof String);
        Assertions.assertEquals("0102FF", fieldValue);
    }

    @Test
    void testGeographyStringConversion() throws Exception {
        SeaTunnelRowDebeziumDeserializationConverters converters =
                new SeaTunnelRowDebeziumDeserializationConverters(
                        new SeaTunnelRowType(
                                new String[] {"geo"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}),
                        new MetadataConverter[] {},
                        ZoneId.systemDefault(),
                        DebeziumDeserializationConverterFactory.DEFAULT);

        byte[] wkb = new byte[] {0x01, 0x02, (byte) 0xFF};
        Schema geographySchema = Geography.builder().optional().build();
        Schema recordSchema = SchemaBuilder.struct().field("geo", geographySchema).build();

        Struct geographyValue = Geometry.createValue(geographySchema, wkb, 4549);
        Struct recordValue = new Struct(recordSchema);
        recordValue.put("geo", geographyValue);

        SourceRecord record =
                new SourceRecord(
                        new HashMap<>(),
                        new HashMap<>(),
                        "topicName",
                        null,
                        SchemaBuilder.int32().build(),
                        1,
                        recordSchema,
                        recordValue,
                        null,
                        new ArrayList<>());

        SeaTunnelRow row = converters.convert(record, recordValue, recordSchema);
        Object fieldValue = row.getField(0);
        Assertions.assertTrue(fieldValue instanceof String);
        Assertions.assertEquals("0102FF", fieldValue);
    }

    /**
     * Verifies the fallback chain in {@code parseOffsetDateTimeFromString} against the four
     * Debezium timestamp formats called out in the review:
     *
     * <ol>
     *   <li>Standard ISO-8601 with numeric offset (baseline)
     *   <li>Oracle TIMESTAMP WITH LOCAL TIME ZONE — IANA zone-region id
     *   <li>MySQL TIMESTAMP in certain schema-history modes — space date/time separator
     *   <li>PostgreSQL timestamptz — short-form hour-only offset
     * </ol>
     *
     * All four variants must parse to the same UTC instant.
     */
    @Test
    void testParseOffsetDateTimeFromStringFallbackChain() {
        ZoneId serverTz = ZoneId.of("Asia/Shanghai");
        // Expected UTC instant: 2024-01-01 04:00:00Z  (2024-01-01 12:00:00+08:00)
        OffsetDateTime expected = Instant.parse("2024-01-01T04:00:00Z").atOffset(ZoneOffset.UTC);

        // 1. ISO-8601 with numeric offset — handled by OffsetDateTime.parse
        OffsetDateTime r1 =
                SeaTunnelRowDebeziumDeserializationConverters.parseOffsetDateTimeFromString(
                        "2024-01-01T12:00:00+08:00", serverTz);
        Assertions.assertEquals(expected, r1, "ISO-8601 numeric offset failed");

        // 2. IANA zone-region id — Oracle TIMESTAMP WITH LOCAL TIME ZONE
        OffsetDateTime r2 =
                SeaTunnelRowDebeziumDeserializationConverters.parseOffsetDateTimeFromString(
                        "2024-01-01T12:00:00 Asia/Shanghai", serverTz);
        Assertions.assertEquals(expected, r2, "IANA zone-region id failed");

        // 3. Space separator — MySQL in certain schema-history modes
        OffsetDateTime r3 =
                SeaTunnelRowDebeziumDeserializationConverters.parseOffsetDateTimeFromString(
                        "2024-01-01 12:00:00+08:00", serverTz);
        Assertions.assertEquals(expected, r3, "Space date/time separator failed");

        // 4. Short-form hour-only offset — PostgreSQL timestamptz
        OffsetDateTime r4 =
                SeaTunnelRowDebeziumDeserializationConverters.parseOffsetDateTimeFromString(
                        "2024-01-01T12:00:00+08", serverTz);
        Assertions.assertEquals(expected, r4, "Short-form hour-only offset failed");
    }

    @Test
    void testParseOffsetDateTimeFromStringThrowsOnUnknownFormat() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        SeaTunnelRowDebeziumDeserializationConverters.parseOffsetDateTimeFromString(
                                "not-a-timestamp", ZoneId.systemDefault()));
    }

    // WKB samples from the experiment database, SUBSTR(HEX(col), 9) of SRID 4326 values.
    private static final String WKB_P = "01010000000000000000605E403333333333333F40";
    private static final String WKB_G = "01010000009A99999999195D403333333333F34340";
    private static final String WKB_LS =
            "0102000000020000000000000000605E403333333333333F406666666666665E40CDCCCCCCCC4C3F40";
    private static final String WKB_POLY =
            "010300000001000000050000009A99999999595E403333333333333F400000000000605E40"
                    + "3333333333333F400000000000605E40CDCCCCCCCC4C3F409A99999999595E40CDCCCCCC"
                    + "CC4C3F409A99999999595E403333333333333F40";
    private static final String WKB_MP =
            "01040000000200000001010000000000000000605E403333333333333F4001010000006666666666665E40"
                    + "CDCCCCCCCC4C3F40";
    private static final String WKB_MLS =
            "0105000000020000000102000000020000000000000000605E403333333333333F406666666666665E40"
                    + "CDCCCCCCCC4C3F40010200000002000000CDCCCCCCCC6C5E406666666666663F403333333333"
                    + "735E400000000000803F40";
    private static final String WKB_MPOLY =
            "010600000001000000010300000001000000050000009A99999999595E403333333333333F40000000"
                    + "0000605E403333333333333F400000000000605E40CDCCCCCCCC4C3F409A99999999595E40CD"
                    + "CCCCCCCC4C3F409A99999999595E403333333333333F40";
    private static final String WKB_GC =
            "01070000000200000001010000000000000000605E403333333333333F40010200000002000000000000"
                    + "0000605E403333333333333F406666666666665E40CDCCCCCCCC4C3F40";

    private static byte[] hexToBytes(String hex) {
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    private static SeaTunnelRow convertRecord(
            SeaTunnelRowType rowType, Schema recordSchema, Struct recordValue) throws Exception {
        SeaTunnelRowDebeziumDeserializationConverters converters =
                new SeaTunnelRowDebeziumDeserializationConverters(
                        rowType,
                        new MetadataConverter[] {},
                        ZoneId.systemDefault(),
                        DebeziumDeserializationConverterFactory.DEFAULT);
        SourceRecord record =
                new SourceRecord(
                        new HashMap<>(),
                        new HashMap<>(),
                        "topicName",
                        null,
                        SchemaBuilder.int32().build(),
                        1,
                        recordSchema,
                        recordValue,
                        null,
                        new ArrayList<>());
        return converters.convert(record, recordValue, recordSchema);
    }

    /** Converts a record with a single geometry column named {@code g}. */
    private static Object convertSingleGeometry(
            GeometryKind kind, Schema geometrySchema, Struct geometryValue) throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"g"}, new SeaTunnelDataType[] {new GeometryType(kind)});
        Schema recordSchema = SchemaBuilder.struct().field("g", geometrySchema).build();
        Struct recordValue = new Struct(recordSchema);
        if (geometryValue != null) {
            recordValue.put("g", geometryValue);
        }
        return convertRecord(rowType, recordSchema, recordValue).getField(0);
    }

    private static void assertGeometry(Object actual, long expectedSrid, byte[] expectedWkb) {
        Assertions.assertTrue(
                actual instanceof org.apache.seatunnel.api.table.type.Geometry,
                "expected SeaTunnel Geometry but got " + actual);
        org.apache.seatunnel.api.table.type.Geometry geometry =
                (org.apache.seatunnel.api.table.type.Geometry) actual;
        Assertions.assertEquals(expectedSrid, geometry.getSrid());
        Assertions.assertArrayEquals(expectedWkb, geometry.getWkb());
    }

    @Test
    void testGeometryStructToGeometry() throws Exception {
        byte[] wkb = hexToBytes(WKB_POLY);
        Schema schema = Geometry.builder().optional().build();
        Object value =
                convertSingleGeometry(
                        GeometryKind.POLYGON, schema, Geometry.createValue(schema, wkb, 4326));
        assertGeometry(value, 4326L, wkb);
    }

    @Test
    void testPointStructToGeometry() throws Exception {
        byte[] wkb = hexToBytes(WKB_P);
        Schema schema = Point.builder().optional().build();
        Object value =
                convertSingleGeometry(
                        GeometryKind.POINT, schema, Point.createValue(schema, wkb, 4326));
        assertGeometry(value, 4326L, wkb);
    }

    @Test
    void testGeometrySrid0PassedThrough() throws Exception {
        Schema schema = Geometry.builder().optional().build();
        byte[] wkb = hexToBytes(WKB_P);
        Struct value = Geometry.createValue(schema, wkb, null);
        Object actual = convertSingleGeometry(GeometryKind.GEOMETRY, schema, value);
        assertGeometry(actual, 0L, wkb);
    }

    @Test
    void testGeometryUnsignedSridPassedThrough() throws Exception {
        Schema schema = Geometry.builder().optional().build();
        byte[] wkb = hexToBytes(WKB_P);
        int[] rawSrids = {0, 3857, 4326, Integer.MAX_VALUE, Integer.MIN_VALUE, -1};
        for (int rawSrid : rawSrids) {
            Struct value = Geometry.createValue(schema, wkb, rawSrid);
            Object actual = convertSingleGeometry(GeometryKind.GEOMETRY, schema, value);
            assertGeometry(actual, Integer.toUnsignedLong(rawSrid), wkb);
        }
    }

    @Test
    void testGeometrySqlNull() throws Exception {
        Schema schema = Geometry.builder().optional().build();
        Assertions.assertNull(convertSingleGeometry(GeometryKind.GEOMETRY, schema, null));
    }

    @Test
    void testFullGeometryRowConversionAndBytesSize() throws Exception {
        String[] names = {"id", "g", "p", "ls", "poly", "mp", "mls", "mpoly", "gc"};
        GeometryKind[] kinds = {
            GeometryKind.GEOMETRY,
            GeometryKind.POINT,
            GeometryKind.LINESTRING,
            GeometryKind.POLYGON,
            GeometryKind.MULTIPOINT,
            GeometryKind.MULTILINESTRING,
            GeometryKind.MULTIPOLYGON,
            GeometryKind.GEOMETRYCOLLECTION
        };
        String[] samples = {WKB_G, WKB_P, WKB_LS, WKB_POLY, WKB_MP, WKB_MLS, WKB_MPOLY, WKB_GC};

        SeaTunnelDataType<?>[] types = new SeaTunnelDataType<?>[names.length];
        types[0] = BasicType.LONG_TYPE;
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA);
        Schema[] geometrySchemas = new Schema[kinds.length];
        for (int i = 0; i < kinds.length; i++) {
            types[i + 1] = new GeometryType(kinds[i]);
            geometrySchemas[i] =
                    kinds[i] == GeometryKind.POINT
                            ? Point.builder().optional().build()
                            : Geometry.builder().optional().build();
            schemaBuilder.field(names[i + 1], geometrySchemas[i]);
        }
        SeaTunnelRowType rowType = new SeaTunnelRowType(names, types);
        Schema recordSchema = schemaBuilder.build();

        Struct fullValue = new Struct(recordSchema);
        fullValue.put("id", 1L);
        int expectedSize = 8;
        for (int i = 0; i < kinds.length; i++) {
            byte[] wkb = hexToBytes(samples[i]);
            expectedSize += wkb.length;
            Struct geometryValue =
                    kinds[i] == GeometryKind.POINT
                            ? Point.createValue(geometrySchemas[i], wkb, 4326)
                            : Geometry.createValue(geometrySchemas[i], wkb, 4326);
            fullValue.put(names[i + 1], geometryValue);
        }

        SeaTunnelRow fullRow = convertRecord(rowType, recordSchema, fullValue);
        Assertions.assertEquals(1L, fullRow.getField(0));
        for (int i = 0; i < kinds.length; i++) {
            assertGeometry(fullRow.getField(i + 1), 4326L, hexToBytes(samples[i]));
        }
        Assertions.assertEquals(expectedSize, fullRow.getBytesSize(rowType));

        Struct nullValue = new Struct(recordSchema);
        nullValue.put("id", 2L);
        SeaTunnelRow nullRow = convertRecord(rowType, recordSchema, nullValue);
        for (int i = 0; i < kinds.length; i++) {
            Assertions.assertNull(nullRow.getField(i + 1), names[i + 1]);
        }
        Assertions.assertEquals(8, nullRow.getBytesSize(rowType));
    }
}
