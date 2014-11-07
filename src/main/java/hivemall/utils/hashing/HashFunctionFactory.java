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
package hivemall.utils.hashing;


import java.util.Random;

public final class HashFunctionFactory {

    public static HashFunction[] create(int numFunctions) {
        return create(numFunctions, 31L);
    }

    public static HashFunction[] create(int numFunctions, long seed) {
        final Random rand = new Random(seed);
        final HashFunction[] funcs = new HashFunction[numFunctions];
        for(int i = 0; i < numFunctions; i++) {
            funcs[i] = new MurmurHash3Function(rand.nextInt());
        }
        return funcs;
    }

}
