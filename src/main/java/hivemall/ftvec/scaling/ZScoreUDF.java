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
package hivemall.ftvec.scaling;

import static hivemall.utils.hadoop.WritableUtils.val;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.FloatWritable;

/**
 * @see <a href="http://en.wikipedia.org/wiki/Standard_score">Standard_score</a>
 */
@Description(name = "zscore", value = "_FUNC_(value, mean, stddev) - Returns a standard score (zscore)")
@UDFType(deterministic = true, stateful = false)
public final class ZScoreUDF extends UDF {

    public FloatWritable evaluate(float value, double mean, double stddev) {
        return evaluate(value, (float) mean, (float) stddev);
    }

    public FloatWritable evaluate(float value, float mean, float stddev) {
        return val((value - mean) / stddev);
    }

}
