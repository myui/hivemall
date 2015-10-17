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
package hivemall;

import hivemall.io.FeatureValue;
import hivemall.utils.hadoop.WritableUtils;
import hivemall.utils.lang.CommandLineUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Writable;

public abstract class UDTFWithOptions extends GenericUDTF {

    protected abstract Options getOptions();

    protected final CommandLine parseOptions(String optionValue) throws UDFArgumentException {
        String[] args = optionValue.split("\\s+");
        Options opts = getOptions();
        return CommandLineUtils.parseOptions(args, opts);
    }

    protected abstract CommandLine processOptions(ObjectInspector[] argOIs)
            throws UDFArgumentException;

    protected final List<FeatureValue> parseFeatures(final List<?> features, final ObjectInspector featureInspector, final boolean parseFeature) {
        final int numFeatures = features.size();
        if(numFeatures == 0) {
            return Collections.emptyList();
        }
        final List<FeatureValue> list = new ArrayList<FeatureValue>(numFeatures);
        for(Object f : features) {
            if(f == null) {
                continue;
            }
            final FeatureValue fv;
            if(parseFeature) {
                fv = FeatureValue.parse(f);
            } else {
                Object o = ObjectInspectorUtils.copyToStandardObject(f, featureInspector, ObjectInspectorCopyOption.WRITABLE);
                Writable k = WritableUtils.toWritable(o);
                fv = new FeatureValue(k, 1.f);
            }
            list.add(fv);
        }
        return list;
    }

}
