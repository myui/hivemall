package hivemall.utils.lang.mutable;

import hivemall.utils.lang.Copyable;

import java.io.Serializable;

public final class MutableInt extends Number
        implements Copyable<MutableInt>, Comparable<MutableInt>, Serializable {
    private static final long serialVersionUID = -3289272606407100628L;

    private int value;

    public MutableInt() {
        super();
    }

    public MutableInt(int value) {
        super();
        this.value = value;
    }

    public MutableInt(Number value) {
        super();
        this.value = value.intValue();
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public void setValue(Number value) {
        this.value = value.intValue();
    }

    @Override
    public int intValue() {
        return value;
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
    public void copyTo(MutableInt probe) {
        probe.setValue(value);
    }

    @Override
    public int compareTo(MutableInt other) {
        return Integer.compare(value, other.value);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof MutableInt) {
            return value == ((MutableInt) obj).intValue();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

}
