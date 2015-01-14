/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
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
package hivemall.utils.lang;

import java.text.DecimalFormat;

public final class NumberUtils {

    private NumberUtils() {}

    public static int parseInt(String s) {
        int endIndex = s.length() - 1;
        char last = s.charAt(endIndex);
        if(Character.isLetter(last)) {
            String numstr = s.substring(0, endIndex);
            int i = Integer.parseInt(numstr);
            switch(last) {
                case 'k':
                case 'K':
                    i *= 1000;
                    break;
                case 'm':
                case 'M':
                    i *= 1000000;
                    break;
                case 'g':
                case 'G':
                    i *= 1000000000;
                    break;
                default:
                    throw new NumberFormatException("Invalid number format: " + s);
            }
            return i;
        } else {
            return Integer.parseInt(s);
        }
    }

    public static int parseInt(String s, int defaultValue) {
        if(s == null) {
            return defaultValue;
        }
        return parseInt(s);
    }

    public static String formatNumber(final long number) {
        DecimalFormat f = new DecimalFormat("#,###");
        return f.format(number);
    }

    public static String prettySize(long size) {
        if(size < 0) {
            return "N/A";
        } else {
            if(size < 1024) {
                return size + " bytes";
            } else {
                float kb = size / 1024f;
                if(kb < 1024f) {
                    return String.format("%.1f KiB", kb);
                } else {
                    float mb = kb / 1024f;
                    if(mb < 1024f) {
                        return String.format("%.1f MiB", mb);
                    } else {
                        float gb = mb / 1024f;
                        return String.format("%.2f GiB", gb);
                    }
                }
            }
        }
    }

}
