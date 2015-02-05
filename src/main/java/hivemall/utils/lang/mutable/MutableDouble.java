package hivemall.utils.lang.mutable;

import hivemall.utils.lang.Copyable;

import java.io.Serializable;

public final class MutableDouble extends Number
        implements Copyable<MutableDouble>, Comparable<MutableDouble>, Serializable {
    private static final long serialVersionUID = 3275291486084936953L;

    private double value;

    public MutableDouble() {
        super();
    }

    public MutableDouble(double value) {
        super();
        this.value = value;
    }

    public MutableDouble(Number value) {
        super();
        this.value = value.doubleValue();
    }

    public void addValue(double o) {
        value += o;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public void setValue(Number value) {
        this.value = value.doubleValue();
    }

    @Override
    public int intValue() {
        return (int) value;
    }

    @Override
    public long longValue() {
        return (long) value;
    }

    @Override
    public float floatValue() {
        return (float) value;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    @Override
    public void copyTo(MutableDouble another) {
        another.setValue(value);
    }

    @Override
    public void copyFrom(MutableDouble another) {
        this.value = another.value;
    }

    @Override
    public int compareTo(MutableDouble other) {
        return Double.compare(value, other.value);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof MutableDouble)
                && (Double.doubleToLongBits(((MutableDouble) obj).value) == Double.doubleToLongBits(value));
    }

    @Override
    public int hashCode() {
        long bits = Double.doubleToLongBits(value);
        return (int) (bits ^ (bits >>> 32));
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

}
