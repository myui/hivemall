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

/**
 * StopWatch provides a API for timings.
 */
public final class StopWatch {

    private final String label;
    private long begin = 0;
    private long end = 0;
    private boolean showInSec = false;

    public StopWatch() {
        this(null, false);
    }

    public StopWatch(String label) {
        this(label, false);
    }

    public StopWatch(String label, boolean showInSec) {
        this.label = label;
        this.showInSec = showInSec;
        start();
    }

    public void setShowInSec(boolean showInSec) {
        this.showInSec = showInSec;
    }

    public void start() {
        begin = System.currentTimeMillis();
    }

    public long stop() {
        end = System.currentTimeMillis();
        return end - begin;
    }

    public void suspend() {
        end = System.currentTimeMillis();
    }

    public void resume() {
        begin += (System.currentTimeMillis() - end);
    }

    public void reset() {
        begin = 0;
        end = 0;
    }

    public long elapsed() {
        if(end != 0) {
            return end - begin;
        } else {
            return System.currentTimeMillis() - begin;
        }
    }

    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        if(label != null) {
            buf.append(label + ": ");
        }
        long t = elapsed();
        if(showInSec) {
            buf.append(DateTimeFormatter.formatTimeInSec(t));
            buf.append("sec");
        } else {
            buf.append(DateTimeFormatter.formatTime(t));
        }
        return buf.toString();
    }

}
