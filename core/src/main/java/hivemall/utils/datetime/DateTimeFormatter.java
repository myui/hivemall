/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.utils.datetime;

import java.text.DecimalFormat;

public final class DateTimeFormatter {

    private DateTimeFormatter() {}

    public static String formatTime(long t) {
        if (t == 0L) {
            return "0ms";
        }
        final StringBuilder buf = new StringBuilder();
        final long hour = t / 3600000;
        if (hour > 0) {
            buf.append(hour + "h ");
            t = t % 3600000;
        }
        final long min = t / 60000;
        if (min > 0) {
            buf.append(min + "m ");
            t = t % 60000;
        }
        final long sec = t / 1000;
        if (sec > 0) {
            buf.append(sec + "s ");
            t = t % 1000;
        }
        if (t > 0) {
            buf.append(t + "ms");
        }
        return buf.length() == 0 ? "0ms" : buf.toString();
    }

    public static String formatTime(double timeInMills) {
        if (timeInMills == 0d) {
            return "0ms";
        }
        final StringBuilder buf = new StringBuilder();
        long t = (long) timeInMills;
        float diff = (float) (timeInMills - t);
        final long hour = t / 3600000;
        if (hour > 0) {
            buf.append(hour + "h ");
            t = t % 3600000;
        }
        final long min = t / 60000;
        if (min > 0) {
            buf.append(min + "m ");
            t = t % 60000;
        }
        final long sec = t / 1000;
        if (sec > 0) {
            buf.append(sec + "s ");
            t = t % 1000;
        }
        if (t > 0 || diff > 0f) {
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
