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
package hivemall.utils.datetime;

import java.text.DecimalFormat;

public final class DateTimeFormatter {

    private DateTimeFormatter() {}

    public static String formatTime(long t) {
        if(t == 0L) {
            return "0ms";
        }
        final StringBuilder buf = new StringBuilder();
        final long hour = t / 3600000;
        if(hour > 0) {
            buf.append(hour + "h ");
            t = t % 3600000;
        }
        final long min = t / 60000;
        if(min > 0) {
            buf.append(min + "m ");
            t = t % 60000;
        }
        final long sec = t / 1000;
        if(sec > 0) {
            buf.append(sec + "s ");
            t = t % 1000;
        }
        if(t > 0) {
            buf.append(t + "ms");
        }
        return buf.length() == 0 ? "0ms" : buf.toString();
    }

    public static String formatTime(double timeInMills) {
        if(timeInMills == 0d) {
            return "0ms";
        }
        final StringBuilder buf = new StringBuilder();
        long t = (long) timeInMills;
        float diff = (float) (timeInMills - t);
        final long hour = t / 3600000;
        if(hour > 0) {
            buf.append(hour + "h ");
            t = t % 3600000;
        }
        final long min = t / 60000;
        if(min > 0) {
            buf.append(min + "m ");
            t = t % 60000;
        }
        final long sec = t / 1000;
        if(sec > 0) {
            buf.append(sec + "s ");
            t = t % 1000;
        }
        if(t > 0 || diff > 0f) {
            buf.append(String.format("%.2f", (diff + t)));
            buf.append("ms");
        }
        return buf.length() == 0 ? "0ms" : buf.toString();
    }

    public static String formatNanoTime(final long t) {
        final long ms = t / 1000000L;
        return formatTime(ms);
    }

    public static String formatTimeInSec(long mills) {
        double sec = mills / 1000d;
        return formatNumber(sec, false);
    }

    public static String formatTimeInSec(double mills) {
        double sec = mills / 1000d;
        return formatNumber(sec, false);
    }

    private static String formatNumber(final double number, boolean commaSep) {
        DecimalFormat f = new DecimalFormat(commaSep ? "#,###.###" : "###.###");
        f.setDecimalSeparatorAlwaysShown(false);
        return f.format(number);
    }

}
