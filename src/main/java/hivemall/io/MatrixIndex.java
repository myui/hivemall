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
package hivemall.io;

public final class MatrixIndex {

    private int i, j;

    public MatrixIndex(int i, int j) {
        this.i = i;
        this.j = j;
    }

    public int getRowIndex() {
        return i;
    }

    public void setRowIndex(int i) {
        this.i = i;
    }

    public int getColIndex() {
        return j;
    }

    public void setColIndex(int j) {
        this.j = j;
    }

    @Override
    public int hashCode() {
        return 31 * (31 + i) + j;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj != null && obj instanceof MatrixIndex) {
            MatrixIndex other = (MatrixIndex) obj;
            return (other.i == this.i) && (other.j == this.j);
        }
        return false;
    }

    @Override
    public String toString() {
        return "MatrixIndex [i=" + i + ", j=" + j + "]";
    }

}
