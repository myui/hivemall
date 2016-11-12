/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.tools;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.LongWritable;

@Description(name = "x_rank",
        value = "_FUNC_(KEY) - Generates a pseudo sequence number starting from 1 for each key")
@UDFType(deterministic = false, stateful = true)
public final class RankSequenceUDF extends UDF {

    private final LongWritable counter = new LongWritable(0L);
    private boolean lastNull = false;
    private String lastKey = null;

    @Nonnull
    public LongWritable evaluate(@Nullable final String key) {
        if (key == null) {
            if (lastNull) {
                counter.set(counter.get() + 1L);
            } else {
                counter.set(1L);
                this.lastNull = true;
            }
        } else {
            if (key.equals(lastKey)) {
                counter.set(counter.get() + 1L);
            } else {
                counter.set(1L);
                this.lastKey = key;
            }
            this.lastNull = false;
        }
        return counter;
    }

}
