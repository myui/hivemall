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
package hivemall.mix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntAvgAccumulator implements Accumulatable<Float> {

    private int sum;
    private int count;

    public IntAvgAccumulator() {
        reset();
    }

    @Override
    public void reset() {
        this.sum = 0;
        this.count = 0;
    }

    @Override
    public void accumulate(DataInput in) throws IOException {
        this.sum += in.readInt();
        this.count += in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(sum);
        out.writeInt(count);
    }

    @Override
    public Float get() {
        return Float.valueOf(sum / count);
    }

}
