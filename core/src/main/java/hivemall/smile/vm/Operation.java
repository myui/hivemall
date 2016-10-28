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
package hivemall.smile.vm;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class Operation {

    final OperationEnum op;
    final String operand;

    public Operation(@Nonnull OperationEnum op) {
        this(op, null);
    }

    public Operation(@Nonnull OperationEnum op, @Nullable String operand) {
        this.op = op;
        this.operand = operand;
    }

    public enum OperationEnum {
        ADD, SUB, DIV, MUL, DUP, // reserved
        PUSH, POP, GOTO, IFEQ, IFEQ2, IFGE, IFGT, IFLE, IFLT, CALL; // used

        static OperationEnum valueOfLowerCase(String op) {
            return OperationEnum.valueOf(op.toUpperCase());
        }
    }

    @Override
    public String toString() {
        return op.toString() + (operand != null ? (" " + operand) : "");
    }

}
