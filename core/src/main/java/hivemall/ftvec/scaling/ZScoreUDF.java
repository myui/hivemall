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
package hivemall.ftvec.scaling;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.FloatWritable;

/**
 * @see <a href="http://en.wikipedia.org/wiki/Standard_score">Standard_score</a>
 */
@Description(name = "zscore",
        value = "_FUNC_(value, mean, stddev) - Returns a standard score (zscore)")
@UDFType(deterministic = true, stateful = false)
public final class ZScoreUDF extends UDF {

    public FloatWritable evaluate(double value, double mean, double stddev) {
        if (stddev == 0.d) {
            return new FloatWritable(0.f);
        }
        float v = (float) ((value - mean) / stddev);
        return new FloatWritable(v);
    }

    public FloatWritable evaluate(float value, float mean, float stddev) {
        if (stddev == 0.f) {
            return new FloatWritable(0.f);
        }
        float v = (value - mean) / stddev;
        return new FloatWritable(v);
    }

}
