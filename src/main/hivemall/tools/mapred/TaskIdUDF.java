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

import static hivemall.utils.WritableUtils.val;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;

@Description(name = "taskid", value = "_FUNC_() - Returns the value of mapred.task.partition")
@UDFType(deterministic = false, stateful = true)
public class TaskIdUDF extends UDF {

    /**  
     * @since Hive 0.12.0
     */
    public IntWritable evaluate() {
        return val(getTaskId());
    }

    public static int getTaskId() {
        MapredContext ctx = MapredContext.get();
        JobConf conf = ctx.getJobConf();
        int taskid = conf.getInt("mapred.task.partition", -1);
        if(taskid == -1) {
            throw new IllegalStateException("mapred.task.partition is not set");
        }
        return taskid;
    }

}
