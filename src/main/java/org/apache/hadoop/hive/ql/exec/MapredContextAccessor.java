/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
package org.apache.hadoop.hive.ql.exec;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.mapred.JobConf;

/**
 * An accessor to allow access to {@link MapredContext}
 * 
 * @link https://issues.apache.org/jira/browse/HIVE-4737
 */
public final class MapredContextAccessor {

    @Nullable
    public static MapredContext get() {
        return MapredContext.get();
    }

    @Nonnull
    public static MapredContext create(boolean isMap, @Nullable JobConf jobConf) {
        return MapredContext.init(isMap, jobConf);
    }

}
