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
package hivemall.matrix;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public final class CSRMatrixBuilder {

    private final IntList columnIndices;
    private final DoubleList values;
    private final IntList rowPointers;

    private int numRows;
    private int numColums;
    private int maxNumColumns;

    public CSRMatrixBuilder(int initsize) {
        this.columnIndices = new IntArrayList(initsize);
        this.values = new DoubleArrayList(initsize);
        this.rowPointers = new IntArrayList(initsize);
        this.numRows = 0;
        this.numColums = -1;
        this.maxNumColumns = -1;
    }

    public void nextRow() {
        ++numRows;
        int ptr = columnIndices.size();
        rowPointers.add(ptr);
        this.maxNumColumns = Math.max(numColums, maxNumColumns);
        this.numColums = 0;
    }

    public void nextColumn(int feature, double value) {
        columnIndices.add(feature);
        values.add(value);
        numColums++;
    }
    
    public CSRMatrix buildMatrix() {
        // TODO
        return null;
    }

}
