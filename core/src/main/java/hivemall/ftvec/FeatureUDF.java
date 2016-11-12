/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.ftvec;

import hivemall.utils.hadoop.HiveUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

@Description(
        name = "feature",
        value = "_FUNC_(<string|int|long|short|byte> feature, <number> value) - Returns a feature string")
@UDFType(deterministic = true, stateful = false)
public final class FeatureUDF extends GenericUDF {

    @Nullable
    private Text _result;

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 2) {
            throw new UDFArgumentException(
                "_FUNC_ takes exactly 2 arguments, feature index and value");
        }
        validateFeatureOI(argOIs[0]);
        validateValueOI(argOIs[1]);

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    private static void validateFeatureOI(@Nonnull ObjectInspector argOI)
            throws UDFArgumentException {
        if (!HiveUtils.isPrimitiveOI(argOI)) {
            throw new UDFArgumentException(
                "_FUNC_ expects integer type or string for `feature` but got "
                        + argOI.getTypeName());
        }
        final PrimitiveObjectInspector oi = (PrimitiveObjectInspector) argOI;
        switch (oi.getPrimitiveCategory()) {
            case INT:
            case SHORT:
            case LONG:
            case BYTE:
            case STRING:
                break;
            default: {
                throw new UDFArgumentException(
                    "_FUNC_ expects integer type or string for `feature` but got "
                            + argOI.getTypeName());
            }
        }
    }

    private static void validateValueOI(@Nonnull ObjectInspector argOI) throws UDFArgumentException {
        if (!HiveUtils.isNumberOI(argOI)) {
            throw new UDFArgumentException("_FUNC_ expects a number type for `value` but got "
                    + argOI.getTypeName());
        }
    }

    @Override
    @Nullable
    public Text evaluate(@Nonnull DeferredObject[] args) throws HiveException {
        assert (args.length == 2) : args.length;

        final Object arg0 = args[0].get();
        if (arg0 == null) {
            return null;
        }
        final Object arg1 = args[1].get();
        if (arg1 == null) {
            return null;
        }

        // arg0|arg1 is Primitive Java object or Writable
        // Then, toString() works fine
        String featureStr = arg0.toString();
        String valueStr = arg1.toString();
        String fv = featureStr + ':' + valueStr;

        Text result = this._result;
        if (result == null) {
            result = new Text(fv);
            this._result = result;
        } else {
            result.set(fv);
        }

        return result;
    }

    @Override
    public String getDisplayString(@Nonnull String[] children) {
        return "feature(" + children[0] + ", " + children[1] + ")";
    }

}
