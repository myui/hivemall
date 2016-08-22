/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
package hivemall.anomaly;

import hivemall.anomaly.ChangeFinderUDF.ChangeFinder;
import hivemall.anomaly.ChangeFinderUDF.Parameters;
import hivemall.utils.hadoop.HiveUtils;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

final class ChangeFinder2D implements ChangeFinder {

    @Nonnull
    private final ListObjectInspector listOI;
    @Nonnull
    private final PrimitiveObjectInspector elemOI;

    ChangeFinder2D(@Nonnull Parameters params, @Nonnull ListObjectInspector listOI)
            throws UDFArgumentTypeException {
        this.listOI = listOI;
        this.elemOI = HiveUtils.asDoubleCompatibleOI(listOI.getListElementObjectInspector());
        
    }

    @Override
    public void update(@Nonnull final Object arg, @Nonnull final double[] outScores) {
        HiveUtils.asDoubleArray(arg, listOI, elemOI);
        
    }

}
