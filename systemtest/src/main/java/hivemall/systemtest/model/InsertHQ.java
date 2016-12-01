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
package hivemall.systemtest.model;

import hivemall.utils.lang.Preconditions;

import javax.annotation.Nonnull;
import java.util.List;

public class InsertHQ extends TableHQ {
    @Nonnull
    public final List<Object[]> data;
    @Nonnull
    public final List<String> header;

    InsertHQ(@Nonnull final String tableName, @Nonnull final List<String> header,
            @Nonnull final List<Object[]> data) {
        super(tableName);

        int l = 0;
        for (Object[] objs : data) {
            Preconditions.checkArgument(objs.length == header.size(),
                "l.%s : Mismatch between number of elements in row(%s) and length of header(%s)",
                l, objs.length, header.size());
            l++;
        }

        this.data = data;
        this.header = header;
    }
}
