/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2015
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

/**
 * Adding feature indices to a dense feature.
 * <pre>
 * Usage: select add_feature_index(array(3,4.0,5)) from dual;
 * > ["1:3.0","2:4.0","3:5.0"]
 * </pre>
 */
@Description(name = "add_feature_index", value = "_FUNC_(ARRAY[DOUBLE]: dense feature vector) - Returns a feature vector with feature indicies")
@UDFType(deterministic = true, stateful = false)
public final class AddFeatureIndexUDF extends UDF {

    public List<Text> evaluate(List<Double> ftvec) {
        if(ftvec == null) {
            return null;
        }
        int size = ftvec.size();
        if(size == 0) {
            return Collections.emptyList();
        }
        final Text[] array = new Text[size];
        for(int i = 0; i < size; i++) {
            Double v = ftvec.get(i);
            array[i] = new Text(String.valueOf(i + 1) + ':' + v);
        }
        return Arrays.asList(array);
    }

}
