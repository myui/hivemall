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
package hivemall.systemtest.runner;

import javax.annotation.Nonnull;

public class SystemTestCommonInfo {
    @Nonnull
    public final String baseDir;
    @Nonnull
    public final String caseDir;
    @Nonnull
    public final String answerDir;
    @Nonnull
    public final String initDir;
    @Nonnull
    public final String dbName;

    public SystemTestCommonInfo(@Nonnull final Class<?> clazz) {
        baseDir = clazz.getName().replace(".", "/");
        caseDir = baseDir + "/case/";
        answerDir = baseDir + "/answer/";
        initDir = baseDir + "/init/";
        dbName = clazz.getName().replace(".", "_").toLowerCase();
    }
}
