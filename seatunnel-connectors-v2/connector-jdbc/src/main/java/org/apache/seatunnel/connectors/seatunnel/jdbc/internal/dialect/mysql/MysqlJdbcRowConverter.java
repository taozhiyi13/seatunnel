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

import org.apache.seatunnel.api.table.type.Geometry;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcFieldTypeUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;

public class MysqlJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return DatabaseIdentifier.MYSQL;
    }

    @Override
    protected LocalTime readTime(ResultSet rs, int resultSetIndex) throws SQLException {
        return JdbcFieldTypeUtils.getLocalTime(rs, resultSetIndex);
    }

    @Override
    protected void writeTime(PreparedStatement statement, int index, LocalTime time)
            throws SQLException {
        // Write to time column using timestamp retains milliseconds
        statement.setTimestamp(
                index, java.sql.Timestamp.valueOf(LocalDateTime.of(LocalDate.now(), time)));
    }

    /**
     * MySQL returns a geometry as a little-endian unsigned 32-bit SRID followed by standard WKB.
     */
    @Override
    protected Object readGeometry(ResultSet rs, int resultSetIndex) throws SQLException {
        byte[] raw = rs.getBytes(resultSetIndex);
        if (raw == null) {
            return null;
        }
        if (raw.length < Integer.BYTES) {
            throw new SQLException(
                    "MySQL geometry value at column "
                            + resultSetIndex
                            + " is shorter than the 4-byte SRID prefix");
        }
        long srid =
                Integer.toUnsignedLong(
                        (raw[0] & 0xFF)
                                | ((raw[1] & 0xFF) << 8)
                                | ((raw[2] & 0xFF) << 16)
                                | ((raw[3] & 0xFF) << 24));
        return new Geometry(srid, Arrays.copyOfRange(raw, Integer.BYTES, raw.length));
    }
}
