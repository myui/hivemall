package hivemall.utils.lang.mutable;

import hivemall.utils.lang.Copyable;

import java.io.Serializable;

public final class MutableLong extends Number
        implements Copyable<MutableLong>, Comparable<MutableLong>, Serializable {
    private static final long serialVersionUID = 4215176730382645660L;

    private long value;

    public MutableLong() {
        super();
    }

    public MutableLong(long value) {
        super();
        this.value = value;
    }

    public MutableLong(Number value) {
        super();
        this.value = value.longValue();
    }

    public void addValue(long o) {
        value += o;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public void setValue(Number value) {
        this.value = value.longValue();
    }

    @Override
    public int intValue() {
        return (int) value;
    }

    @Override
    public long longValue() {
        return value;
    }

    @Override
    public float floatValue() {
        return value;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    @Override
    public void copyTo(MutableLong another) {
        another.setValue(value);
    }

    @Override
    public void copyFrom(MutableLong another) {
        this.value = another.value;
    }

    @Override
    public int compareTo(MutableLong other) {
        return compare(value, other.value);
    }

    private static int compare(final long x, final long y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof MutableLong) {
            return value == ((MutableLong) obj).longValue();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

}
