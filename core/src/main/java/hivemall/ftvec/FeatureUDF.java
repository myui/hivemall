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
package hivemall.ftvec;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

@Description(name = "feature",
        value = "_FUNC_(string feature, double weight) - Returns a feature string")
@UDFType(deterministic = true, stateful = false)
public final class FeatureUDF extends GenericUDF {
    private PrimitiveObjectInspector.PrimitiveCategory featureCategory;
    private PrimitiveObjectInspector.PrimitiveCategory weightCategory;

    private ObjectInspectorConverters.Converter converter;

    private ObjectInspector featureOI;
    private ObjectInspector weightOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors)
            throws UDFArgumentException {
        if (objectInspectors.length != 2) {
            throw new UDFArgumentException("_FUNC_ takes exactly 2 arguments, features label and weight");
        }

        if (!(objectInspectors[0] instanceof PrimitiveObjectInspector)) {
            throw new UDFArgumentException("Expected numeric type or string but got "
                    + objectInspectors[0].getTypeName());
        }

        if (!(objectInspectors[1] instanceof PrimitiveObjectInspector)) {
            throw new UDFArgumentException("Expected numeric type but got "
                    + objectInspectors[1].getTypeName());
        }

        featureCategory = parsePrimitiveCategory(objectInspectors[0], false);
        featureOI = objectInspectors[0];
        weightCategory = parsePrimitiveCategory(objectInspectors[1], true);
        weightOI = objectInspectors[1];

        converter = ObjectInspectorConverters.getConverter(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    private PrimitiveObjectInspector.PrimitiveCategory parsePrimitiveCategory(ObjectInspector oi, boolean isWeight)
            throws UDFArgumentException {
        if (oi instanceof IntObjectInspector) {
            return PrimitiveObjectInspector.PrimitiveCategory.INT;
        } else if (oi instanceof LongObjectInspector) {
            return PrimitiveObjectInspector.PrimitiveCategory.LONG;
        } else if (oi instanceof FloatObjectInspector) {
            return PrimitiveObjectInspector.PrimitiveCategory.FLOAT;
        } else if (oi instanceof DoubleObjectInspector) {
            return PrimitiveObjectInspector.PrimitiveCategory.DOUBLE;
        } else if (oi instanceof StringObjectInspector && !isWeight) {
            return PrimitiveObjectInspector.PrimitiveCategory.STRING;
        } else {
            throw new UDFArgumentException("Expected numeric or string type but got "
                    + oi.getTypeName());
        }
    }

    private String parseAsString(PrimitiveObjectInspector.PrimitiveCategory category, Object value) {
        if (category == PrimitiveObjectInspector.PrimitiveCategory.INT) {
            return ((Integer)value).toString();
        } else if (category == PrimitiveObjectInspector.PrimitiveCategory.LONG) {
            return ((Long)value).toString();
        } else if (category == PrimitiveObjectInspector.PrimitiveCategory.FLOAT) {
            return ((Float)value).toString();
        } else if (category == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
            return ((Double)value).toString();
        } else if (category == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            return ((String)value);
        } else {
            return null;
        }
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (args.length != 2) {
            return null;
        }

        if (args[0].get() == null || args[1].get() == null) {
            return null;
        }

        if (featureOI == null || weightOI == null) {
            return new HiveException("Invalid ObjectInspector");
        }

        Object feature = ((PrimitiveObjectInspector)featureOI)
                .getPrimitiveJavaObject(args[0].get());
        Object weight = ((PrimitiveObjectInspector)weightOI)
                .getPrimitiveJavaObject(args[1].get());

        String featureStr = parseAsString(featureCategory, feature);
        String weightStr = parseAsString(weightCategory, weight);

        return converter.convert(featureStr + ":" + weightStr);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "feature(" + strings[0] + ", " + strings[1] + ")";
    }

}
