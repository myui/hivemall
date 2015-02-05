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
