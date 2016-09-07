/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hivemall.tools.mapred;

import java.util.UUID;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/** An alternative implementation of [[hivemall.tools.mapred.RowIdUDF]]. */
@Description(
        name = "rowid",
        value = "_FUNC_() - Returns a generated row id of a form {TASK_ID}-{UUID}-{SEQUENCE_NUMBER}")
@UDFType(deterministic = false, stateful = true)
public class RowIdUDFWrapper extends GenericUDF {
    // RowIdUDF is directly used because spark cannot
    // handle HadoopUtils#getTaskId().

    private long sequence;
    private long taskId;

    public RowIdUDFWrapper() {
        this.sequence = 0L;
        this.taskId = Thread.currentThread().getId();
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 0) {
            throw new UDFArgumentLengthException("row_number() has no argument.");
        }

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 0);
        sequence++;
        /**
         * TODO: Check if it is unique over all tasks in executors of Spark.
         */
        return taskId + "-" + UUID.randomUUID() + "-" + sequence;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "row_number()";
    }
}
