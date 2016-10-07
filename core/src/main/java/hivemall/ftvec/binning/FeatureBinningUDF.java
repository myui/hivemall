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
package hivemall.ftvec.binning;

import hivemall.utils.hadoop.HiveUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.*;

@Description(
        name = "feature_binning",
        value = "_FUNC_(array<features::string> features, const map<string, array<number>> quantiles_map)"
                + " / _FUNC(number weight, const array<number> quantiles)"
                + " - Returns binned features as an array<features::string> / bin ID as int")
@UDFType(deterministic = true, stateful = false)
public final class FeatureBinningUDF extends GenericUDF {
    private boolean multiple = true;

    private ListObjectInspector featuresOI;
    private StringObjectInspector featureOI;
    private MapObjectInspector quantilesMapOI;
    private StringObjectInspector keyOI;
    private ListObjectInspector quantilesOI;
    private PrimitiveObjectInspector quantileOI;

    private PrimitiveObjectInspector weightOI;

    private Map<Text, double[]> quantilesMap = null;
    private double[] quantiles = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] OIs) throws UDFArgumentException {
        if (OIs.length != 2) {
            throw new UDFArgumentLengthException("Specify two arguments");
        }

        if (HiveUtils.isListOI(OIs[0]) && HiveUtils.isMapOI(OIs[1])) {
            // for (array<features::string> features, const map<string, array<number>> quantiles_map)

            if (!HiveUtils.isStringOI(((ListObjectInspector) OIs[0]).getListElementObjectInspector())) {
                throw new UDFArgumentTypeException(0,
                    "Only array<string> type argument is acceptable but " + OIs[0].getTypeName()
                            + " was passed as `features`");
            }
            featuresOI = HiveUtils.asListOI(OIs[0]);
            featureOI = HiveUtils.asStringOI(featuresOI.getListElementObjectInspector());

            quantilesMapOI = HiveUtils.asMapOI(OIs[1]);
            if (!HiveUtils.isStringOI(quantilesMapOI.getMapKeyObjectInspector())
                    || !HiveUtils.isListOI(quantilesMapOI.getMapValueObjectInspector())
                    || !HiveUtils.isNumberOI(((ListObjectInspector) quantilesMapOI.getMapValueObjectInspector()).getListElementObjectInspector())) {
                throw new UDFArgumentTypeException(1,
                    "Only map<string, array<number>> type argument is acceptable but "
                            + OIs[1].getTypeName() + " was passed as `quantiles_map`");
            }
            keyOI = HiveUtils.asStringOI(quantilesMapOI.getMapKeyObjectInspector());
            quantilesOI = HiveUtils.asListOI(quantilesMapOI.getMapValueObjectInspector());
            quantileOI = HiveUtils.asDoubleCompatibleOI(quantilesOI.getListElementObjectInspector());

            multiple = true;

            return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        } else if (HiveUtils.isPrimitiveOI(OIs[0]) && HiveUtils.isListOI(OIs[1])) {
            // for (number weight, const array<number> quantiles)

            weightOI = HiveUtils.asDoubleCompatibleOI(OIs[0]);

            quantilesOI = HiveUtils.asListOI(OIs[1]);
            if (!HiveUtils.isNumberOI(quantilesOI.getListElementObjectInspector())) {
                throw new UDFArgumentTypeException(1,
                    "Only array<number> type argument is acceptable but " + OIs[1].getTypeName()
                            + " was passed as `quantiles`");
            }
            quantileOI = HiveUtils.asDoubleCompatibleOI(quantilesOI.getListElementObjectInspector());

            multiple = false;

            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        } else {
            throw new UDFArgumentTypeException(0,
                "Only <array<features::string>, map<string, array<number>>> "
                        + "or <number, array<number>> type arguments are accepted but <"
                        + OIs[0].getTypeName() + ", " + OIs[1].getTypeName() + "> was passed.");
        }
    }

    @Override
    public Object evaluate(DeferredObject[] dObj) throws HiveException {
        if (multiple) {
            // init quantilesMap
            if (quantilesMap == null) {
                quantilesMap = new HashMap<Text, double[]>();
                final Map<?, ?> _quantilesMap = quantilesMapOI.getMap(dObj[1].get());

                for (Object _key : _quantilesMap.keySet()) {
                    final Text key = new Text(keyOI.getPrimitiveJavaObject(_key));
                    final double[] val = HiveUtils.asDoubleArray(_quantilesMap.get(key),
                        quantilesOI, quantileOI);
                    quantilesMap.put(key, val);
                }
            }

            final List<?> fs = featuresOI.getList(dObj[0].get());
            final List<Text> result = new ArrayList<Text>();
            for (Object f : fs) {
                final String entry = featureOI.getPrimitiveJavaObject(f);
                final int pos = entry.indexOf(":");

                if (pos < 0) {
                    // categorical
                    result.add(new Text(entry));
                } else {
                    // quantitative
                    final Text key = new Text(entry.substring(0, pos));
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

    private int findBin(double[] _quantiles, double d) throws HiveException {
        if (_quantiles.length < 3) {
            throw new HiveException(
                "Length of `quantiles` should be greater than or equal to three but "
                        + _quantiles.length + ".");
        }

        int res = Arrays.binarySearch(_quantiles, d);
        return (res < 0) ? ~res - 1 : (res == 0) ? 0 : res - 1;
    }

    @Override
    public String getDisplayString(String[] children) {
        final StringBuilder sb = new StringBuilder();
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
