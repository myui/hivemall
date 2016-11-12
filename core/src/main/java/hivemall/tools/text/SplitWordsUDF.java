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
package hivemall.tools.text;

import hivemall.utils.hadoop.WritableUtils;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

@Description(
        name = "split_words",
        value = "_FUNC_(string query [, string regex]) - Returns an array<text> containing splitted strings")
@UDFType(deterministic = true, stateful = false)
public final class SplitWordsUDF extends UDF {

    public List<Text> evaluate(String query) {
        return evaluate(query, "[\\s ]+");
    }

    public List<Text> evaluate(String query, String regex) {
        if (query == null) {
            return null;
        }
        String[] words = query.split(regex, -1);
        return WritableUtils.val(words);
    }

}
