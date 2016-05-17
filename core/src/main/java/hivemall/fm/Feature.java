/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hivemall.fm;


import hivemall.utils.hashing.MurmurHash3;
import hivemall.utils.lang.NumberUtils;

import java.nio.ByteBuffer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;

public abstract class Feature {
    public static final int NUM_FIELDS = 1024;
    /** 2^21 */
    public static final int NUM_FEATURES = 2097152;
    public static final int FEATURE_INDEX_RIGHT_OPEN_BOUND = NUM_FEATURES + NUM_FIELDS;

    protected double value;

    public Feature() {}

    public Feature(double value) {
        this.value = value;
    }

    public void setFeature(@Nonnull String f) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    public String getFeature() {
        throw new UnsupportedOperationException();
    }

    public void setFeatureIndex(int i) {
        throw new UnsupportedOperationException();
    }

    public int getFeatureIndex() {
        throw new UnsupportedOperationException();
    }

    public short getField() {
        throw new UnsupportedOperationException();
    }

    public void setField(short field) {
        throw new UnsupportedOperationException();
    }

    public double getValue() {
        return value;
    }

    public abstract int bytes();

    public abstract void writeTo(@Nonnull ByteBuffer dst);

    public abstract void readFrom(@Nonnull ByteBuffer src);

    public static int requiredBytes(@Nonnull final Feature[] x) {
        int ret = 0;
        for (Feature f : x) {
            assert (f != null);
            ret += f.bytes();
        }
        return ret;
    }

    @Nullable
    public static Feature[] parseFeatures(@Nonnull final Object arg,
            @Nonnull final ListObjectInspector listOI, @Nullable final Feature[] probes,
            final boolean asIntFeature) throws HiveException {
        if (arg == null) {
            return null;
        }

        final int length = listOI.getListLength(arg);
        final Feature[] ary;
        if (probes != null && probes.length == length) {
            ary = probes;
        } else {
            ary = new Feature[length];
        }

        int j = 0;
        for (int i = 0; i < length; i++) {
            Object o = listOI.getListElement(arg, i);
            if (o == null) {
                continue;
            }
            String s = o.toString();
            Feature f = ary[j];
            if (f == null) {
                f = parseFeature(s, asIntFeature);
            } else {
                parseFeature(s, f, asIntFeature);
            }
            ary[j] = f;
            j++;
        }
        if (j == length) {
            return ary;
        } else {
            Feature[] dst = new Feature[j];
            System.arraycopy(ary, 0, dst, 0, j);
            return dst;
        }
    }

    public static Feature[] parseFFMFeatures(@Nonnull final Object arg,
            @Nonnull final ListObjectInspector listOI, @Nullable final Feature[] probes)
            throws HiveException {
        if (arg == null) {
            return null;
        }

        final int length = listOI.getListLength(arg);
        final Feature[] ary;
        if (probes != null && probes.length == length) {
            ary = probes;
        } else {
            ary = new Feature[length];
        }

        int j = 0;
        for (int i = 0; i < length; i++) {
            Object o = listOI.getListElement(arg, i);
            if (o == null) {
                continue;
            }
            String s = o.toString();
            Feature f = ary[j];
            if (f == null) {
                f = parseFFMFeature(s);
            } else {
                parseFFMFeature(s, f);
            }
            ary[j] = f;
            j++;
        }
        if (j == length) {
            return ary;
        } else {
            Feature[] dst = new Feature[j];
            System.arraycopy(ary, 0, dst, 0, j);
            return dst;
        }
    }

    @Nonnull
    static Feature parseFeature(@Nonnull final String fv, final boolean asIntFeature)
            throws HiveException {
        final int pos1 = fv.indexOf(':');
        if (pos1 == -1) {
            if (asIntFeature) {
                int index = parseFeatureIndex(fv);
                return new IntFeature(index, 1.d);
            } else {
                return new StringFeature(/* index */fv, 1.d);
            }
        } else {
            final String indexStr = fv.substring(0, pos1);
            final String valueStr = fv.substring(pos1 + 1);
            if (asIntFeature) {
                int index = parseFeatureIndex(indexStr);
                double value = parseFeatureValue(valueStr);
                return new IntFeature(index, value);
            } else {
                double value = parseFeatureValue(valueStr);
                return new StringFeature(/* index */indexStr, value);
            }
        }
    }

    @Nonnull
    static IntFeature parseFFMFeature(@Nonnull final String fv) throws HiveException {
        final int pos1 = fv.indexOf(':');
        if (pos1 == -1) {
            throw new HiveException("Invalid FFM feature format: " + fv);
        }
        final String lead = fv.substring(0, pos1);
        final String rest = fv.substring(pos1 + 1);
        final int pos2 = rest.indexOf(':');
        if (pos2 == -1) {// e.g., i1:1.0 (quantitative features)
            final int index;
            if (NumberUtils.isDigits(lead)) {
                index = parseFeatureIndex(lead);
                if (index < 0 || index >= NUM_FIELDS) {
                    throw new HiveException("Invalid index value '" + index
                            + "' for a quantative features: " + fv + ", expecting index less than "
                            + NUM_FIELDS);
                }
            } else {
                index = MurmurHash3.murmurhash3(lead, NUM_FIELDS);
            }
            short field = (short) index;
            double value = parseFeatureValue(rest);
            return new IntFeature(index, field, value);
        }

        final String indexStr = rest.substring(0, pos2);
        final int index;
        final short field;
        if (NumberUtils.isDigits(indexStr) && NumberUtils.isDigits(lead)) {
            index = parseFeatureIndex(indexStr);
            if (index >= FEATURE_INDEX_RIGHT_OPEN_BOUND) {
                throw new HiveException("Feature index MUST be less than "
                        + FEATURE_INDEX_RIGHT_OPEN_BOUND + " but was " + index);
            }
            field = parseField(lead);
        } else {
            // +NUM_FIELD to avoid conflict to quantitative features
            index = MurmurHash3.murmurhash3(indexStr, NUM_FEATURES) + NUM_FIELDS;
            field = (short) MurmurHash3.murmurhash3(lead, NUM_FIELDS);
        }
        String valueStr = rest.substring(pos2 + 1);
        double value = parseFeatureValue(valueStr);

        return new IntFeature(index, field, value);
    }

    static void parseFeature(@Nonnull final String fv, @Nonnull final Feature probe,
            final boolean asIntFeature) throws HiveException {
        final int pos1 = fv.indexOf(":");
        if (pos1 == -1) {
            if (asIntFeature) {
                int index = parseFeatureIndex(fv);
                probe.setFeatureIndex(index);
            } else {
                probe.setFeature(fv);
            }
            probe.value = 1.d;
        } else {
            final String indexStr = fv.substring(0, pos1);
            final String valueStr = fv.substring(pos1 + 1);
            if (asIntFeature) {
                int index = parseFeatureIndex(indexStr);
                probe.setFeatureIndex(index);
                probe.value = parseFeatureValue(valueStr);;
            } else {
                probe.setFeature(indexStr);
                probe.value = parseFeatureValue(valueStr);
            }
        }
    }

    static void parseFFMFeature(@Nonnull final String fv, @Nonnull final Feature probe)
            throws HiveException {
        final int pos1 = fv.indexOf(":");
        if (pos1 == -1) {
            throw new HiveException("Invalid FFM feature format: " + fv);
        }
        final String lead = fv.substring(0, pos1);
        final String rest = fv.substring(pos1 + 1);
        final int pos2 = rest.indexOf(':');
        if (pos2 == -1) {// e.g., i1:1.0 (quantitative features) expecting |feature| less than 1024
            final int index;
            if (NumberUtils.isDigits(lead)) {
                index = parseFeatureIndex(lead);
                if (index < 0 || index >= NUM_FIELDS) {
                    throw new HiveException("Invalid index value '" + index
                            + "' for a quantative features: " + fv + ", expecting index less than "
                            + NUM_FIELDS);
                }
            } else {
                index = MurmurHash3.murmurhash3(lead, NUM_FIELDS);
            }
            short field = (short) index;
            probe.setField(field);
            probe.setFeatureIndex(index);
            probe.value = parseFeatureValue(rest);
            return;
        }

        String indexStr = rest.substring(0, pos2);
        final int index;
        final short field;
        if (NumberUtils.isDigits(indexStr) && NumberUtils.isDigits(lead)) {
            index = parseFeatureIndex(indexStr);
            if (index >= FEATURE_INDEX_RIGHT_OPEN_BOUND) {
                throw new HiveException("Feature index MUST be less than "
                        + FEATURE_INDEX_RIGHT_OPEN_BOUND + " but was " + index);
            }
            field = parseField(lead);
        } else {
            // +NUM_FIELD to avoid conflict to quantitative features
            index = MurmurHash3.murmurhash3(indexStr, NUM_FEATURES) + NUM_FIELDS;
            field = (short) MurmurHash3.murmurhash3(lead, NUM_FIELDS);
        }
        probe.setField(field);
        probe.setFeatureIndex(index);

        String valueStr = rest.substring(pos2 + 1);
        probe.value = parseFeatureValue(valueStr);
    }


    private static int parseFeatureIndex(@Nonnull final String indexStr) throws HiveException {
        final int index;
        try {
            index = Integer.parseInt(indexStr);
        } catch (NumberFormatException e) {
            throw new HiveException("Invalid index value: " + indexStr, e);
        }
        if (index < 0) {
            throw new HiveException("Feature index MUST be greater than 0: " + indexStr);
        }
        return index;
    }

    private static double parseFeatureValue(@Nonnull final String value) throws HiveException {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new HiveException("Invalid feature value: " + value, e);
        }
    }

    private static short parseField(@Nonnull final String fieldStr) throws HiveException {
        final short field;
        try {
            field = Short.parseShort(fieldStr);
        } catch (NumberFormatException e) {
            throw new HiveException("Invalid field value: " + fieldStr, e);
        }
        if (field < 0 || field >= NUM_FIELDS) {
            throw new HiveException("Invalid field value: " + fieldStr);
        }
        return field;
    }

    public static int toIntFeature(@Nonnull final Feature x, final int yField) {
        int index = x.getFeatureIndex();
        return index * NUM_FIELDS + yField;
    }

}
