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
