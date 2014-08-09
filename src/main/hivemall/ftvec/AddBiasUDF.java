/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
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

@Description(name = "add_bias", value = "_FUNC_(feature_vector in array<string>) - Returns features with a bias in array<string>")
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
