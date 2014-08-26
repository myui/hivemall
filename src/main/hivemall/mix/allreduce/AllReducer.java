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
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This code is based on https://github.com/y-tag/java-Hadoop-AdditionalNetwork
 */
package hivemall.mix.allreduce;

import hivemall.mix.Accumulatable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class AllReducer<T extends Accumulatable<?>> {

    public AllReducer() {}

    public void allreduce(AllReduceContext context, T accumulator) throws IOException {
        reduce(context, accumulator);
        broadcast(context, accumulator);
    }

    protected void reduce(AllReduceContext context, T accumulator) throws IOException {
        // read data from children
        for(DataInputStream childIn : context.getChildrenDataInputStreams()) {
            accumulator.accumulate(childIn);
        }
        // write data to parent
        if(!context.isRoot()) {
            DataOutputStream parentOut = context.getParentDataOutputStream();
            accumulator.write(parentOut);
            parentOut.flush();
        }
    }

    protected void broadcast(AllReduceContext context, T accumulator) throws IOException {
        // read data from parent
        if(!context.isRoot()) {
            accumulator.reset();
            DataInputStream parentIn = context.getParentDataInputStream();
            accumulator.accumulate(parentIn);
        }
        // write data to children
        for(DataOutputStream childOut : context.getChildrenDataOutputStreams()) {
            // TODO: threading
            accumulator.write(childOut);
            childOut.flush();
        }
    }

}
