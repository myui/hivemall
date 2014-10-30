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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

@Description(name = "rowid", value = "_FUNC_() - Returns a generated row id of a form {TASK_ID}-{SEQUENCE_NUMBER}")
@UDFType(deterministic = false, stateful = true)
public class RowIdUDF extends UDF {

    private long sequence;
    private int taskId;

    public RowIdUDF() {
        this.sequence = 0L;
        this.taskId = -1;
    }

    public Text evaluate() {
        if(taskId == -1) {
            this.taskId = HadoopUtils.getTaskId() + 1;
        }
        sequence++;
        String rowid = taskId + "-" + sequence;
        return val(rowid);
    }
}
