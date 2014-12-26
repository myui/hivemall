package hivemall.utils.lang.mutable;

import hivemall.utils.lang.Copyable;

import java.io.Serializable;

public final class MutableFloat extends Number
        implements Copyable<MutableFloat>, Comparable<MutableFloat>, Serializable {
    private static final long serialVersionUID = 1758508142164954048L;

    private float value;

    public MutableFloat() {
        super();
    }

    public MutableFloat(float value) {
        super();
        this.value = value;
    }

    public MutableFloat(Number value) {
        super();
        this.value = value.floatValue();
    }

    public void addValue(float o) {
        value += o;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    public void setValue(Number value) {
        this.value = value.floatValue();
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
        return value;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    @Override
    public void copyTo(MutableFloat another) {
        another.setValue(value);
    }

    @Override
    public void copyFrom(MutableFloat another) {
        this.value = another.value;
    }

    @Override
    public int compareTo(MutableFloat other) {
        return Float.compare(value, other.value);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof MutableFloat)
                && (Float.floatToIntBits(((MutableFloat) obj).value) == Float.floatToIntBits(value));
    }

    @Override
    public int hashCode() {
        return Float.floatToIntBits(value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

}
