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
