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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * A spatial value: an unsigned 32-bit SRID plus standard WKB without the MySQL 4-byte SRID prefix.
 */
public final class Geometry implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final long MIN_SRID = 0L;
    public static final long MAX_SRID = 0xFFFF_FFFFL;

    private static final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();

    private final long srid;
    private final byte[] wkb;

    public Geometry(long srid, byte[] wkb) {
        if (srid < MIN_SRID || srid > MAX_SRID) {
            throw new IllegalArgumentException(
                    "SRID must be an unsigned 32-bit value, but was " + srid);
        }
        this.srid = srid;
        this.wkb = Objects.requireNonNull(wkb, "wkb");
    }

    public long getSrid() {
        return srid;
    }

    public byte[] getWkb() {
        return wkb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Geometry)) {
            return false;
        }
        Geometry that = (Geometry) o;
        return srid == that.srid && Arrays.equals(wkb, that.wkb);
    }

    @Override
    public int hashCode() {
        return 31 * Long.hashCode(srid) + Arrays.hashCode(wkb);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(wkb.length * 2 + 32);
        sb.append("Geometry{srid=").append(srid).append(", wkb=");
        for (byte b : wkb) {
            sb.append(HEX_DIGITS[(b >> 4) & 0x0F]).append(HEX_DIGITS[b & 0x0F]);
        }
        return sb.append('}').toString();
    }
}
