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
package hivemall.utils;

import hivemall.io.FeatureValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.io.Text;

public final class HivemallUtils {

    private HivemallUtils() {}

    @Nonnull
    public static List<FeatureValue> parseTextFeaturesAsString(@Nonnull final List<Text> args) {
        final int size = args.size();
        if(size == 0) {
            return Collections.emptyList();
        }
        final FeatureValue[] array = new FeatureValue[size];
        for(int i = 0; i < size; i++) {
            Text t = args.get(i);
            if(t == null) {
                continue;
            }
            FeatureValue fv = FeatureValue.parseFeatureAsString(t);
            array[i] = fv;
        }
        return Arrays.asList(array);
    }

}
