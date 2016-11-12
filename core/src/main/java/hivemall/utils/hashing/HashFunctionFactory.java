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
package hivemall.utils.hashing;


import java.util.Random;

public final class HashFunctionFactory {

    public static HashFunction[] create(int numFunctions) {
        return create(numFunctions, 31L);
    }

    public static HashFunction[] create(int numFunctions, long seed) {
        final Random rand = new Random(seed);
        final HashFunction[] funcs = new HashFunction[numFunctions];
        for (int i = 0; i < numFunctions; i++) {
            funcs[i] = new MurmurHash3Function(rand.nextInt());
        }
        return funcs;
    }

}
