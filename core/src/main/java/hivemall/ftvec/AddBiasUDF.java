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

import hivemall.HivemallConstants;
import hivemall.utils.hadoop.WritableUtils;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@Description(
        name = "add_bias",
        value = "_FUNC_(feature_vector in array<string>) - Returns features with a bias in array<string>")
@UDFType(deterministic = true, stateful = false)
public final class AddBiasUDF extends UDF {

    public List<Text> evaluate(List<String> ftvec) {
        return evaluate(ftvec, HivemallConstants.BIAS_CLAUSE);
    }

    public List<Text> evaluate(List<String> ftvec, String biasClause) {
        float biasValue = 1.f;
        return evaluate(ftvec, biasClause, biasValue);
    }

    public List<Text> evaluate(List<String> ftvec, String biasClause, float biasValue) {
        int size = ftvec.size();
        String[] newvec = new String[size + 1];
        ftvec.toArray(newvec);
        newvec[size] = biasClause + ":" + Float.toString(biasValue);
        return WritableUtils.val(newvec);
    }

    public List<IntWritable> evaluate(List<IntWritable> ftvec, IntWritable biasClause) {
        int size = ftvec.size();
        IntWritable[] newvec = new IntWritable[size + 1];
        ftvec.toArray(newvec);
        newvec[size] = biasClause;
        return Arrays.asList(newvec);
    }

}
