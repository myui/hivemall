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
package hivemall.smile.tools;

import hivemall.utils.hadoop.HiveUtils;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(name = "guess_attribute_types", value = "_FUNC_(ANY, ...) - Returns attribute types"
        + "\nselect guess_attribute_types(*) from train limit 1;"
        + "\n> Q,Q,C,C,C,C,Q,C,C,C,Q,C,Q,Q,Q,Q,C,Q")
@UDFType(deterministic = true, stateful = false)
public final class GuessAttributesUDF extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final StringBuilder buf = new StringBuilder(128);
        final int numArgs = argOIs.length;
        final int last = numArgs - 1;
        for(int i = 0; i < numArgs; i++) {
            if(HiveUtils.isNumberOI(argOIs[i])) {
                buf.append('Q'); // quantitative
            } else {
                buf.append('C'); // categorical            
            }
            if(i != last) {
                buf.append(',');
            }
        }
        String value = buf.toString();
        return ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, value);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        throw new HiveException("GuessAttributesUDF#evaluation should not be called");
    }

    @Override
    public String getDisplayString(String[] children) {
        return "guess_attribute_types(" + Arrays.toString(children) + ')';
    }

}
