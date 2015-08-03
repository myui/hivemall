/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
package hivemall.smile;

import java.util.Arrays;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import smile.data.Attribute;
import smile.data.DateAttribute;
import smile.data.NominalAttribute;
import smile.data.NumericAttribute;
import smile.data.StringAttribute;
import smile.math.Math;

public final class SmileUtils {

    private SmileUtils() {}

    /**
     * Q for {@link NumericAttribute}, C for {@link NominalAttribute}, S for
     * {@link StringAttribute}, and D for {@link DateAttribute}.
     */
    @Nullable
    public static Attribute[] resolveAttributes(@Nullable final String opt) throws HiveException {
        if(opt == null) {
            return null;
        }
        final String[] opts = opt.split(",");
        final int size = opts.length;
        final Attribute[] attr = new Attribute[size];
        for(int i = 0; i < size; i++) {
            final String type = opts[i];
            if("Q".equals(type)) {
                attr[i] = new NumericAttribute("V" + (i + 1));
            } else if("C".equals(type)) {
                attr[i] = new NominalAttribute("V" + (i + 1));
            } else if("S".equals(type)) {
                attr[i] = new StringAttribute("V" + (i + 1));
            } else if("D".equals(type)) {
                attr[i] = new DateAttribute("V" + (i + 1));
            } else {
                throw new HiveException("Unexpected type: " + type);
            }
        }
        return attr;
    }

    @Nonnull
    public static int[] classLables(@Nonnull final int[] y) throws HiveException {
        final int[] labels = Math.unique(y);
        Arrays.sort(labels);

        if(labels.length < 2) {
            throw new HiveException("Only one class.");
        }
        for(int i = 0; i < labels.length; i++) {
            if(labels[i] < 0) {
                throw new HiveException("Negative class label: " + labels[i]);
            }
            if(i > 0 && labels[i] - labels[i - 1] > 1) {
                throw new HiveException("Missing class: " + labels[i] + 1);
            }
        }

        return labels;
    }

    public static Attribute[] attributeTypes(@Nullable Attribute[] attributes, @Nonnull final double[][] x) {
        if(attributes == null) {
            int p = x[0].length;
            attributes = new Attribute[p];
            for(int i = 0; i < p; i++) {
                attributes[i] = new NumericAttribute("V" + (i + 1));
            }
        }
        return attributes;
    }

}
