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
package hivemall.utils;

import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public final class HiveUtils {

    public static void main(String[] args) {}

    public static Text asText(final Object o) {
        if(o == null) {
            return null;
        }
        if(o instanceof Text) {
            return (Text) o;
        }
        if(o instanceof LazyString) {
            LazyString l = (LazyString) o;
            return l.getWritableObject();
        }
        if(o instanceof String) {
            String s = (String) o;
            return new Text(s);
        }
        String s = o.toString();
        return new Text(s);
    }

    public static int asJavaInt(final Object o, final int nullValue) {
        if(o == null) {
            return nullValue;
        }
        return asJavaInt(o);
    }

    public static int asJavaInt(final Object o) {
        if(o == null) {
            throw new IllegalArgumentException();
        }
        if(o instanceof Integer) {
            return ((Integer) o).intValue();
        }
        if(o instanceof LazyInteger) {
            IntWritable i = ((LazyInteger) o).getWritableObject();
            return i.get();
        }
        if(o instanceof IntWritable) {
            return ((IntWritable) o).get();
        }
        String s = o.toString();
        return Integer.parseInt(s);
    }

}
