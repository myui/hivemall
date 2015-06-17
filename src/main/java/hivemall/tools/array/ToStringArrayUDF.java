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
package hivemall.tools.array;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

@Description(name = "to_string_array", value = "_FUNC_(array<int>) - Returns an array of strings")
@UDFType(deterministic = true, stateful = false)
public final class ToStringArrayUDF extends UDF {

    @Nullable
    public List<Text> evaluate(@Nullable final List<Integer> inArray) {
        if(inArray == null) {
            return null;
        }

        final int size = inArray.size();
        if(size == 0) {
            return Collections.emptyList();
        }

        final List<Text> outArray = new ArrayList<Text>(size);
        for(int i = 0; i < size; i++) {
            Object o = inArray.get(i);
            if(o == null) {
                outArray.add(null);
            } else {
                String s = o.toString();
                outArray.add(new Text(s));
            }
        }
        return outArray;
    }

}
