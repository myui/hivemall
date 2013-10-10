/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.common;

public final class HivemallConstants {

    public static final String BIAS_CLAUSE = "+bias";
    public static final int BIAS_CLAUSE_INT = -1;

    // org.apache.hadoop.hive.serde.Constants (hive 0.9)
    // org.apache.hadoop.hive.serde.serdeConstants (hive 0.10 or later)
    public static final String VOID_TYPE_NAME = "void";
    public static final String BOOLEAN_TYPE_NAME = "boolean";
    public static final String TINYINT_TYPE_NAME = "tinyint";
    public static final String SMALLINT_TYPE_NAME = "smallint";
    public static final String INT_TYPE_NAME = "int";
    public static final String BIGINT_TYPE_NAME = "bigint";
    public static final String FLOAT_TYPE_NAME = "float";
    public static final String DOUBLE_TYPE_NAME = "double";
    public static final String STRING_TYPE_NAME = "string";
    public static final String DATE_TYPE_NAME = "date";
    public static final String DATETIME_TYPE_NAME = "datetime";
    public static final String TIMESTAMP_TYPE_NAME = "timestamp";
    public static final String BINARY_TYPE_NAME = "binary";
    public static final String LIST_TYPE_NAME = "array";
    public static final String MAP_TYPE_NAME = "map";
    public static final String STRUCT_TYPE_NAME = "struct";
    public static final String UNION_TYPE_NAME = "uniontype";

}
