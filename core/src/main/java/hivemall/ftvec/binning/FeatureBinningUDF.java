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
package hivemall.ftvec.binning;

import hivemall.utils.hadoop.HiveUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Description(
        name = "feature_binning",
        value = "_FUNC_(array<features::string> features, const map<string, array<double>> quantiles_map)/(int|bigint|float|double weight, const array<double> quantiles) - Returns binned features as an array<features::string> / bin ID as int")
public class FeatureBinningUDF extends GenericUDF {
    private boolean multiple = true;

    private StandardListObjectInspector featuresOI;
    private WritableStringObjectInspector featuresElOI;
    private StandardMapObjectInspector quantilesMapOI;
    private WritableStringObjectInspector keyOI;
    private StandardListObjectInspector quantilesOI;
    private WritableDoubleObjectInspector quantileOI;

    private PrimitiveObjectInspector weightOI;

    private Map<Text, double[]> quantilesMap = null;
    private double[] quantiles = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] OIs) throws UDFArgumentException {
        if (OIs.length != 2)
            throw new UDFArgumentLengthException("Specify two arguments");

        Category arg0Category = OIs[0].getCategory();
        Category arg1Category = OIs[1].getCategory();

        if (arg0Category == Category.LIST && arg1Category == Category.MAP) {
            featuresOI = (StandardListObjectInspector) OIs[0];
            isStringOI(featuresOI.getListElementObjectInspector(), 0, "features");
            featuresElOI = (WritableStringObjectInspector) featuresOI.getListElementObjectInspector();
            quantilesMapOI = (StandardMapObjectInspector) OIs[1];
            isStringOI(quantilesMapOI.getMapKeyObjectInspector(), 1, "key of quantiles_map");
            keyOI = (WritableStringObjectInspector) quantilesMapOI.getMapKeyObjectInspector();
            isListOI(quantilesMapOI.getMapValueObjectInspector(), 1, "value of quantiles_map");
            quantilesOI = (StandardListObjectInspector) quantilesMapOI.getMapValueObjectInspector();
            isNumberOI(quantilesOI.getListElementObjectInspector(), 1, "value of quantiles");
            quantileOI = (WritableDoubleObjectInspector) quantilesOI.getListElementObjectInspector();

            multiple = true;

            return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        } else if (arg0Category == Category.PRIMITIVE && arg1Category == Category.LIST) {
            isNumberOI(OIs[0], 0, "weight");
            weightOI = (PrimitiveObjectInspector) OIs[0];
            quantilesOI = (StandardListObjectInspector) OIs[1];
            isNumberOI(quantilesOI.getListElementObjectInspector(), 1, "value of quantiles");
            quantileOI = (WritableDoubleObjectInspector) quantilesOI.getListElementObjectInspector();

            multiple = false;

            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        } else {
            throw new UDFArgumentTypeException(
                0,
                "Only <array<features::string>, map<string, array<double>>> "
                        + "or <int|bigint|float|double, array<double>> type arguments are accepted but <"
                        + OIs[0].getTypeName() + ", " + OIs[1].getTypeName() + "> was passed.");
        }
    }

    private boolean isStringOI(ObjectInspector OI, int index, String name)
            throws UDFArgumentTypeException {
        if (HiveUtils.isStringOI(OI))
            return true;
        else
            throw new UDFArgumentTypeException(index,
                "Only string type arguments are accepted but " + OI.getTypeName()
                        + " was passed as `" + name + "`.");
    }

    private boolean isListOI(ObjectInspector OI, int index, String name)
            throws UDFArgumentTypeException {
        if (HiveUtils.isListOI(OI))
            return true;
        else
            throw new UDFArgumentTypeException(index, "Only array type arguments are accepted but "
                    + OI.getTypeName() + " was passed as `" + name + "`.");

    }

    private boolean isNumberOI(ObjectInspector OI, int index, String name)
            throws UDFArgumentTypeException {
        if (HiveUtils.isNumberOI(OI))
            return true;
        else
            throw new UDFArgumentTypeException(index,
                "Only numeric type arguments are accepted but " + OI.getTypeName()
                        + " was passed as `" + name + "`.");
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] dObj) throws HiveException {
        if (multiple) {
            // init quantilesMap
            if (quantilesMap == null) {
                quantilesMap = new HashMap<Text, double[]>();
                Map _quantilesMap = quantilesMapOI.getMap(dObj[1].get());

                for (Object _key : _quantilesMap.keySet()) {
                    Text key = new Text(keyOI.getPrimitiveJavaObject(_key));
                    double[] val = HiveUtils.asDoubleArray(_quantilesMap.get(key), quantilesOI,
                        quantileOI);
                    quantilesMap.put(key, val);
                }
            }

            List fs = featuresOI.getList(dObj[0].get());
            List<Text> result = new ArrayList<Text>();
            for (Object f : fs) {
                String entry = featuresElOI.getPrimitiveJavaObject(f);
                int pos = entry.indexOf(":");

                if (pos < 0) {
                    // categorical
                    result.add(new Text(entry));
                } else {
                    // quantitative
                    Text key = new Text(entry.substring(0, pos));
                    String val = entry.substring(pos + 1);

                    // binning
                    if (quantilesMap.containsKey(key)) {
                        val = String.valueOf(findBin(quantilesMap.get(key), Double.parseDouble(val)));
                    }
                    result.add(new Text(key + ":" + val));
                }
            }

            return result;
        } else {
            // init quantiles
            if (quantiles == null) {
                quantiles = HiveUtils.asDoubleArray(dObj[1].get(), quantilesOI, quantileOI);
            }

            return new IntWritable(findBin(quantiles,
                PrimitiveObjectInspectorUtils.getDouble(dObj[0].get(), weightOI)));
        }
    }

    private int findBin(double[] _quantiles, double target) throws HiveException {
        if (_quantiles.length < 3)
            throw new HiveException(
                "Length of `quantiles` should be greater than or equal to three but "
                        + _quantiles.length + ".");

        int left = 0;
        int right = _quantiles.length - 1;
        int p = (left + right) / 2;
        while (left + 1 != right) {
            if (_quantiles[p] < target) {
                left = p;
                p = (left + right) / 2;
            } else {
                right = p;
                p = (left + right) / 2;
            }
        }
        return p;
    }

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("feature_binning");
        sb.append("(");
        if (children.length > 0) {
            sb.append(children[0]);
            for (int i = 1; i < children.length; i++) {
                sb.append(", ");
                sb.append(children[i]);
            }
        }
        sb.append(")");
        return sb.toString();
    }
}
