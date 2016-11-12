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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

/**
 * Adding feature indices to a dense feature.
 * 
 * <pre>
 * Usage: select add_feature_index(array(3,4.0,5)) from dual;
 * > ["1:3.0","2:4.0","3:5.0"]
 * </pre>
 */
@Description(
        name = "add_feature_index",
        value = "_FUNC_(ARRAY[DOUBLE]: dense feature vector) - Returns a feature vector with feature indicies")
@UDFType(deterministic = true, stateful = false)
public final class AddFeatureIndexUDF extends UDF {

    public List<Text> evaluate(List<Double> ftvec) {
        if (ftvec == null) {
            return null;
        }
        int size = ftvec.size();
        if (size == 0) {
            return Collections.emptyList();
        }
        final Text[] array = new Text[size];
        for (int i = 0; i < size; i++) {
            Double v = ftvec.get(i);
            array[i] = new Text(String.valueOf(i + 1) + ':' + v);
        }
        return Arrays.asList(array);
    }

}
