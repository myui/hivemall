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

/**
 * Unsigned short of range [0,65535].
 */
public final class UnsignedShort extends Number implements Comparable<UnsignedShort> {
    private static final long serialVersionUID = -2656215264326194039L;

    public static final int MIN_VALUE = 0x0000; // 0
    public static final int MAX_VALUE = 0xFFFF; // 65535    
    private static final int DELTA = 32768; // -Short.MIN_VALUE

    private final short underlying;

    public UnsignedShort(short v) {
        this(v & 0xFFFF); // convert to range 0-65535 from -32768-32767.
    }

    public UnsignedShort(int v) {
        rangeCheck(v);
        this.underlying = (short) (v - DELTA);
    }

    private static void rangeCheck(final int value) throws NumberFormatException {
        if(value < MIN_VALUE || value > MAX_VALUE) {
            throw new NumberFormatException("Value is out of range : " + value);
        }
    }

    /**
     * Get the underlying short value of range [-32768,32767]
     */
    @Override
    public short shortValue() {
        return underlying;
    }

    @Override
    public int intValue() {
        return (int) underlying + DELTA;
    }

    @Override
    public long longValue() {
        return intValue();
    }

    @Override
    public float floatValue() {
        return intValue();
    }

    @Override
    public double doubleValue() {
        return intValue();
    }

    @Override
    public int compareTo(UnsignedShort other) {
        return Short.compare(underlying, other.underlying);
    }

}
