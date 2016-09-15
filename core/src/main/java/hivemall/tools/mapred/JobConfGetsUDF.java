/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2016 Makoto YUI
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
package hivemall.tools.mapred;

import static hivemall.utils.hadoop.WritableUtils.val;
import hivemall.utils.hadoop.HadoopUtils;

import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.MapredContextAccessor;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

/**
 * @since Hive 0.12.0
 */
@Description(name = "jobconf_gets", value = "_FUNC_() - Returns the value from JobConf")
@UDFType(deterministic = false, stateful = true)
public class JobConfGetsUDF extends UDF {

    public Text evaluate() throws HiveException {
        return evaluate(null);
    }

    public Text evaluate(@Nullable final String regexKey) throws HiveException {
        MapredContext ctx = MapredContextAccessor.get();
        if (ctx == null) {
            throw new HiveException("MapredContext is not set");
        }
        JobConf jobconf = ctx.getJobConf();
        if (jobconf == null) {
            throw new HiveException("JobConf is not set");
        }

        String dumped = HadoopUtils.toString(jobconf, regexKey);
        return val(dumped);
    }

}
