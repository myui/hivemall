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

import hivemall.utils.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import javax.annotation.Nullable;

public final class StackMachine {

    private final Map<String, Double> valuesMap;
    private final Map<String, Integer> jumpMap;
    private final List<Operation> code;
    private final Stack<Double> programStack;

    private int IP;
    private int SP;

    private int codeLength;
    private boolean[] done;
    private Double result;

    public StackMachine() {
        this.valuesMap = new HashMap<String, Double>();
        this.jumpMap = new HashMap<String, Integer>();
        this.code = new ArrayList<Operation>();
        this.programStack = new Stack<Double>();
        this.SP = 0;
        this.result = null;
    }

    public void run(List<String> scripts, double[] features) throws VMRuntimeException {
        for(String line : scripts) {
            String[] ops = line.split(" ", -1);
            if(ops.length == 2) {
                Operation.OperationEnum o = Operation.OperationEnum.valueOfLowerCase(ops[0]);
                code.add(new Operation(o, ops[1]));
            } else {
                Operation.OperationEnum o = Operation.OperationEnum.valueOfLowerCase(ops[0]);
                code.add(new Operation(o));
            }
        }

        int size = scripts.size();
        this.codeLength = size - 1;
        this.done = new boolean[size];

        bind(features);
        execute(0);
    }

    public void execute(int entryPoint) throws VMRuntimeException {
        valuesMap.put("end", -1.0);
        jumpMap.put("last", codeLength);

        IP = entryPoint;

        while(IP < code.size()) {
            if(done[IP]) {
                throw new VMRuntimeException("There is a infinite loop in the Machine code.");
            }
            done[IP] = true;
            Operation currentOperation = code.get(IP);
            if(!executeOperation(currentOperation)) {
                return;
            }
        }
    }

    public void bind(final double[] features) {
        final StringBuilder buf = new StringBuilder();
        for(int i = 0; i < features.length; i++) {
            String bindKey = buf.append("x[").append(i).append("]").toString();
            valuesMap.put(bindKey, features[i]);
            StringUtils.clear(buf);
        }
    }

    @Nullable
    public Double getResult() {
        return result;
    }

    private Double pop() {
        SP--;
        return programStack.pop();
    }

    private Double push(Double val) {
        programStack.push(val);
        SP++;
        return val;
    }

    private boolean executeOperation(Operation currentOperation) throws VMRuntimeException {
        if(IP < 0) {
            return false;
        }
        switch (currentOperation.op) {
            case GOTO: {
                if(isInt(currentOperation.operand)) {
                    IP = Integer.parseInt(currentOperation.operand);
                } else {
                    IP = jumpMap.get(currentOperation.operand);
                }
                break;
            }
            case CALL: {
                double candidateIP = valuesMap.get(currentOperation.operand);
                if(candidateIP < 0) {
                    evaluateBuiltinByName(currentOperation.operand);
                    IP++;
                }
                break;
            }
            case IFEQ: {
                // follow the rule of smile's Math class.
                double a = pop();
                double b = pop();
                double absa = Math.abs(a);
                double absb = Math.abs(b);
                if(a == b || Math.abs(a - b) <= Math.min(absa, absb) * 2.2204460492503131e-16) {
                    if(isInt(currentOperation.operand)) {
                        IP = Integer.parseInt(currentOperation.operand);
                    } else {
                        IP = jumpMap.get(currentOperation.operand);
                    }
                } else {
                    IP++;
                }
                break;
            }
            case IFGR: {
                double lower = pop();
                double upper = pop();
                if(upper > lower) {
                    if(isInt(currentOperation.operand)) {
                        IP = Integer.parseInt(currentOperation.operand);
                    } else {
                        IP = jumpMap.get(currentOperation.operand);
                    }
                } else {
                    IP++;
                }
                break;
            }
            case POP: {
                valuesMap.put(currentOperation.operand, pop());
                IP++;
                break;
            }
            case PUSH: {
                if(isDouble(currentOperation.operand))
                    push(Double.parseDouble(currentOperation.operand));
                else {
                    Double v = valuesMap.get(currentOperation.operand);
                    if(v == null) {
                        throw new VMRuntimeException("value is not binded: "
                                + currentOperation.operand);
                    }
                    push(v);
                }
                IP++;
                break;
            }
            default:
                throw new VMRuntimeException("Machine code has wrong opcode :"
                        + currentOperation.op);
        }
        return true;

    }

    private void evaluateBuiltinByName(String name) throws VMRuntimeException {
        if(name.equals("end")) {
            this.result = pop();
        } else {
            throw new VMRuntimeException("Machine code has wrong builin function :" + name);
        }
    }

    private static boolean isInt(String i) {
        try {
            Integer.parseInt(i);
            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }

    private static boolean isDouble(String i) {
        try {
            Double.parseDouble(i);
            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }

}
