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
package hivemall.tools.mapred;

import static hivemall.utils.hadoop.WritableUtils.val;
import hivemall.utils.hadoop.HadoopUtils;

import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.MapredContextAccessor;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

/**  
 * @since Hive 0.12.0
 */
@Description(name = "jobconf_gets", value = "_FUNC_() - Returns the value from JobConf")
@UDFType(deterministic = false, stateful = true)
public class JobConfGetsUDF extends UDF {

    public Text evaluate() {
        return evaluate(null);
    }

    public Text evaluate(@Nullable final String regexKey) {
        MapredContext ctx = MapredContextAccessor.get();
        if(ctx == null) {
            throw new IllegalStateException("MapredContext is not set");
        }
        JobConf jobconf = ctx.getJobConf();
        if(jobconf == null) {
            throw new IllegalStateException("JobConf is not set");
        }

        String dumped = HadoopUtils.toString(jobconf, regexKey);
        return val(dumped);
    }

}
