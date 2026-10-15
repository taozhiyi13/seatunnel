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

import java.util.Objects;

/** Data type of a spatial column. Values are {@link Geometry}. */
public final class GeometryType implements SeaTunnelDataType<Geometry> {
    private static final long serialVersionUID = 1L;

    /**
     * Key of {@link org.apache.seatunnel.api.table.catalog.Column#getOptions()} holding the SRID
     * declared on the source spatial column, as an unsigned 32-bit {@link Long}. Absent when the
     * column declares no SRID or the source cannot report it; values in such a column may use any
     * SRID.
     */
    public static final String SRID_OPTION = "srid";

    private final GeometryKind kind;

    public GeometryType(GeometryKind kind) {
        this.kind = Objects.requireNonNull(kind, "kind");
    }

    public GeometryKind getKind() {
        return kind;
    }

    @Override
    public Class<Geometry> getTypeClass() {
        return Geometry.class;
    }

    @Override
    public SqlType getSqlType() {
        return SqlType.GEOMETRY;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GeometryType)) {
            return false;
        }
        return kind == ((GeometryType) o).kind;
    }

    @Override
    public int hashCode() {
        return kind.hashCode();
    }

    @Override
    public String toString() {
        return "GEOMETRY(" + kind + ")";
    }
}
