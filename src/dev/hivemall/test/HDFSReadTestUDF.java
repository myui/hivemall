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
package hivemall.test;

import hivemall.common.WeightValue;
import hivemall.utils.collections.OpenHashMap;
import hivemall.utils.hadoop.HCatalogUtils;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = false)
public class HDFSReadTestUDF extends UDF {

    public Integer evaluate(Text tableName) throws HiveException {
        final OpenHashMap<Object, WeightValue> weights = new OpenHashMap<Object, WeightValue>(1024 * 8);
        try {
            HCatalogUtils.loadWeightsFromHDFS(weights, tableName.toString());
        } catch (IOException e) {
            throw new HiveException(e);
        } catch (Throwable th) {
            th.printStackTrace();
            throw new HiveException(th);
        }
        return weights.size();
    }

}
