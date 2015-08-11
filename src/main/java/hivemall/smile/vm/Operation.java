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
package hivemall.smile.vm;

import javax.annotation.Nonnull;

public final class Operation {

    final OperationEnum op;
    final String operand;

    Operation(@Nonnull OperationEnum op, String operand) {
        this.op = op;
        this.operand = operand;
    }

    Operation(@Nonnull OperationEnum op) {
        this.op = op;
        this.operand = null;
    }

    public enum OperationEnum {
        // FUNCTION is pseudo operation. it will never be executed and don't have IP itself.
        FUNCTION, ADD, SUB, DIV, MUL, DUP, // reserved
        PUSH, POP, GOTO, IFEQ, IFGR, CALL; // used

        static OperationEnum valueOfLowerCase(String op) {
            return OperationEnum.valueOf(op.toUpperCase());
        }
    }

    @Override
    public String toString() {
        return op.toString() + (operand != null ? (" " + operand) : "");
    }

}
