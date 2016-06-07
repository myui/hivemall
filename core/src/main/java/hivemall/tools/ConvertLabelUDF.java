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
package hivemall.tools;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;

@Description(
        name = "convert_label",
        value = "_FUNC_(const int|const float) - Convert from -1|1 to 0.0f|1.0f, or from 0.0f|1.0f to -1|1")
@UDFType(deterministic = true, stateful = false)
public final class ConvertLabelUDF extends UDF {

    public IntWritable evaluate(int label) throws UDFArgumentException {
        if (label == 0) {
            return new IntWritable(-1);
        } else if (label == -1) {
            return new IntWritable(0);
        } else if (label == 1) {
            return new IntWritable(1);
        } else {
            throw new UDFArgumentException("-1 or 1 or 0 is expected. Unexpected label: " + label);
        }
    }

    public IntWritable evaluate(float label) throws UDFArgumentException {
        if (label == 0.f) {
            return new IntWritable(-1);
        } else if (label == -1.f) {
            return new IntWritable(0);
        } else if (label == 1.f) {
            return new IntWritable(1);
        } else {
            throw new UDFArgumentException("-1 or 1 or 0 is expected. Unexpected label: " + label);
        }
    }

    public IntWritable evaluate(double label) throws UDFArgumentException {
        if (label == 0.d) {
            return new IntWritable(-1);
        } else if (label == -1.d) {
            return new IntWritable(0);
        } else if (label == 1.d) {
            return new IntWritable(1);
        } else {
            throw new UDFArgumentException("-1 or 1 or 0 is expected. Unexpected label: " + label);
        }
    }

}
