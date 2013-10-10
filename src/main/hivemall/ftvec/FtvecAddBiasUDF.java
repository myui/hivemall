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

import hivemall.common.HivemallConstants;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;

public class FtvecAddBiasUDF extends UDF {

    public List<String> evaluate(List<String> ftvec) {
        String biasClause = Integer.toString(HivemallConstants.BIAS_CLAUSE_INT);
        return evaluate(ftvec, biasClause);
    }

    public List<String> evaluate(List<String> ftvec, String biasClause) {
        float biasValue = 1.f;
        return evaluate(ftvec, biasClause, biasValue);
    }

    public List<String> evaluate(List<String> ftvec, String biasClause, float biasValue) {
        int size = ftvec.size();
        String[] newvec = new String[size + 1];
        ftvec.toArray(newvec);
        newvec[size] = biasClause + ":" + Float.toString(biasValue);
        return Arrays.asList(newvec);
    }

}
