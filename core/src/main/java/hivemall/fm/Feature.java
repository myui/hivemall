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

import java.nio.ByteBuffer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;

public abstract class Feature {

    protected double value;

    public Feature() {}

    public Feature(double value) {
        this.value = value;
    }

    public void setFeature(String f) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    public String getFeature() {
        throw new UnsupportedOperationException();
    }

    public void setIndex(int i) {
        throw new UnsupportedOperationException();
    }

    public int getIndex() {
        throw new UnsupportedOperationException();
    }

    public void setField(String f) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    public String getField() {
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
                f = parse(s, asIntFeature);
            } else {
                parse(s, f, asIntFeature);
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
    private static Feature parse(@Nonnull final String s, final boolean asIntFeature)
            throws HiveException {
        final int pos = s.indexOf(':');
        if (pos == -1) {
            if (asIntFeature) {
                int index = Integer.parseInt(s);
                if (index < 0) {
                    throw new HiveException("Feature index MUST be greater than 0: " + s);
                }
                return new IntFeature(index, 1.d);
            } else {
                return new StringFeature(s, 1.d);
            }
        } else {
            String s1 = s.substring(0, pos);
            String s2 = s.substring(pos + 1);
            int pos2 = s2.indexOf(':');
            if (pos2 == -1) {
                if (asIntFeature) {
                    int index = Integer.parseInt(s1);
                    if (index < 0) {
                        throw new HiveException("Feature index MUST be greater than 0: " + s);
                    }
                    double value = Double.parseDouble(s2);
                    return new IntFeature(index, value);
                } else {
                    double value = Double.parseDouble(s2);
                    return new StringFeature(s1, value);
                }
            } else {
                String s3 = s2.substring(0, pos2);
                String s4 = s2.substring(pos2 + 1);
                if (asIntFeature) {
                    throw new HiveException("Fields currently unsupported with IntFeatures: " + s);
                } else {
                    double value = Double.parseDouble(s4);
                    return new StringFeature(s3, s1, value);
                }
            }
        }
    }

    private static void parse(@Nonnull final String s, @Nonnull final Feature probe,
            final boolean asIntFeature) throws HiveException {
        final int pos = s.indexOf(":");
        if (pos == -1) {
            if (asIntFeature) {
                int index = Integer.parseInt(s);
                if (index < 0) {
                    throw new HiveException("Feature index MUST be greater than 0: " + s);
                }
                probe.setIndex(index);
            } else {
                probe.setFeature(s);
            }
            probe.value = 1.d;
        } else {
            String s1 = s.substring(0, pos);
            String s2 = s.substring(pos + 1);
            int pos2 = s2.indexOf(':');
            if (pos2 == -1) {
                if (asIntFeature) {
                    int index = Integer.parseInt(s1);
                    if (index < 0) {
                        throw new HiveException("Feature index MUST be greater than 0: " + s);
                    }
                    double value = Double.parseDouble(s2);
                    probe.setIndex(index);
                    probe.value = value;
                } else {
                    probe.setFeature(s1);
                    probe.value = Double.parseDouble(s2);
                }
            } else {
                String s3 = s2.substring(0, pos2);
                String s4 = s2.substring(pos2 + 1);
                if (asIntFeature) {
                    throw new HiveException("Fields currently unsupported with IntFeatures: " + s);
                } else {
                    probe.setField(s3);
                    probe.setFeature(s1);
                    probe.value = Double.parseDouble(s4);
                }
            }
        }
    }

    @Nonnull
    public static Feature createInstance(@Nonnull ByteBuffer src, boolean asIntFeature) {
        if (asIntFeature) {
            return new IntFeature(src);
        } else {
            return new StringFeature(src);
        }
    }
}
