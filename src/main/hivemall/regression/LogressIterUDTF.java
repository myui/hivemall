/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
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
package hivemall.regression;

import hivemall.common.HivemallConstants;
import hivemall.common.WeightValue;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.FloatWritable;

public class LogressIterUDTF extends LogressUDTF {

    private MapObjectInspector featuresWithWeightOI;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        this.featuresWithWeightOI = (MapObjectInspector) argOIs[0];

        return super.initialize(argOIs);
    }

    @Override
    protected ObjectInspector processFeaturesOI(ObjectInspector arg)
            throws UDFArgumentTypeException {
        MapObjectInspector featuresWithWeightOI = (MapObjectInspector) arg;
        ObjectInspector featureOI = featuresWithWeightOI.getMapKeyObjectInspector();
        String keyTypeName = featureOI.getTypeName();
        if(keyTypeName != HivemallConstants.STRING_TYPE_NAME
                && keyTypeName != HivemallConstants.INT_TYPE_NAME
                && keyTypeName != HivemallConstants.BIGINT_TYPE_NAME) {
            throw new UDFArgumentTypeException(0, "1st argument must be Map of key type [Int|BitInt|Text]: "
                    + keyTypeName);
        }
        ObjectInspector weightOI = featuresWithWeightOI.getMapValueObjectInspector();
        if(weightOI.getTypeName() != HivemallConstants.FLOAT_TYPE_NAME) {
            throw new UDFArgumentTypeException(0, "1st argument must be Map of value type Float: "
                    + weightOI.getTypeName());
        }
        return featureOI;
    }

    @Override
    public void process(Object[] args) throws HiveException {
        @SuppressWarnings("unchecked")
        final Map<Object, FloatWritable> featuresWithWeight = (Map<Object, FloatWritable>) featuresWithWeightOI.getMap(args[0]);
        float target = targetOI.get(args[1]);
        checkTargetValue(target);

        final ObjectInspector featureInspector = featuresWithWeightOI.getMapKeyObjectInspector();
        for(Map.Entry<Object, FloatWritable> e : featuresWithWeight.entrySet()) {
            FloatWritable weight = e.getValue();
            if(weight == null) {
                continue;
            }
            Object k = e.getKey();
            Object feature = ObjectInspectorUtils.copyToStandardObject(k, featureInspector);
            if(!weights.containsKey(feature)) {
                float v = weight.get();
                weights.put(feature, new WeightValue(v));
            }
        }

        Set<Object> features = featuresWithWeight.keySet();
        train(features, target);
        count++;
    }

}
