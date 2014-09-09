/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.tools;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

@Description(name = "convert_label", value = "_FUNC_(const int|const float) - Convert from -1|1 to 0.0f|1.0f, or from 0.0f|1.0f to -1|1")
@UDFType(deterministic = true, stateful = false)
public final class ConvertLabelUDF extends UDF {

    public FloatWritable evaluate(int label) throws UDFArgumentException {
        if(label == -1 || label == 0) {
            return new FloatWritable(0.f);
        } else if(label == 1) {
            return new FloatWritable(1.f);
        } else {
            throw new UDFArgumentException("-1 or 1 is expected. Unexpected label: " + label);
        }
    }

    public IntWritable evaluate(float label) throws UDFArgumentException {
        if(label == 0.f) {
            return new IntWritable(-1);
        } else if(label == 1) {
            return new IntWritable(1);
        } else {
            throw new UDFArgumentException("-1 or 1 is expected. Unexpected label: " + label);
        }
    }

}
