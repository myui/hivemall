package hivemall.smile.vm;

import hivemall.utils.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;

public final class StackMachine {

    private HashMap<String, Double> valuesMap;
    private HashMap<String, Integer> jumpMap;
    private ArrayList<Operation> code = new ArrayList<Operation>();
    private Stack<Double> programStack;
    private int IP;
    private int SP = 0;
    private int codeLength;
    private boolean[] done;
    private double result = 0;

    public StackMachine() {}

    public void run(List<String> script, double[] features) throws VMRuntimeException {
        for(String line : script) {
            String[] ops = line.split(" ", -1);
            if(ops.length == 2) {
                Operation.OperationEnum o = Operation.OperationEnum.valueOfLowerCase(ops[0]);
                code.add(new Operation(o, ops[1]));
            } else {
                Operation.OperationEnum o = Operation.OperationEnum.valueOfLowerCase(ops[0]);
                code.add(new Operation(o));
            }
        }

        this.valuesMap = new HashMap<String, Double>();
        this.jumpMap = new HashMap<String, Integer>();
        int size = script.size();
        this.codeLength = size - 1;
        this.done = new boolean[size];

        bind(features);
        execute(0);
    }

    public void execute(int entryPoint) throws VMRuntimeException {
        valuesMap.put("end", -1.0);
        jumpMap.put("last", codeLength);

        IP = entryPoint;
        programStack = new Stack<Double>();
        while(IP < code.size()) {
            if(done[IP]) {
                throw new IllegalArgumentException("There is a infinite loop in the Machine code.");
            }
            done[IP] = true;
            Operation currentOperation = code.get((int) IP);
            if(!executeOperation(currentOperation)) {
                return;
            }
        }
    }

    public void bind(final double[] features) {
        final StringBuilder buf = new StringBuilder();
        for(int i = 0; i < features.length; i++) {
            String bindKey = buf.append("x[").append(String.valueOf(i)).append("]").toString();
            valuesMap.put(bindKey, features[i]);
            StringUtils.clear(buf);
        }
    }

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
        if(IP < 0)
            return false;
        switch (currentOperation.op) {
            case GOTO:
                if(isInt(currentOperation.operand))
                    IP = Integer.parseInt(currentOperation.operand);
                else
                    IP = jumpMap.get(currentOperation.operand);
                break;
            case CALL:
                double candidateIP = valuesMap.get(currentOperation.operand);
                if(candidateIP < 0) {
                    evaluateBuiltinByName(currentOperation.operand);
                    IP++;
                }
                break;
            case IFEQ:
                // follow the rule of smile's Math class.
                double a = pop();
                double b = pop();
                double absa = Math.abs(a);
                double absb = Math.abs(b);
                if(a == b || Math.abs(a - b) <= Math.min(absa, absb) * 2.2204460492503131e-16)
                    if(isInt(currentOperation.operand))
                        IP = Integer.parseInt(currentOperation.operand);
                    else
                        IP = jumpMap.get(currentOperation.operand);
                else
                    IP++;
                break;
            case IFGR:
                double lower = pop();
                double upper = pop();
                if(upper > lower)
                    if(isInt(currentOperation.operand))
                        IP = Integer.parseInt(currentOperation.operand);
                    else
                        IP = jumpMap.get(currentOperation.operand);
                else
                    IP++;
                break;
            case POP:
                valuesMap.put(currentOperation.operand, pop());
                IP++;
                break;
            case PUSH:
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
            default:
                throw new IllegalArgumentException("Machine code has wrong opcode :"
                        + currentOperation.op);
        }
        return true;

    }

    private void evaluateBuiltinByName(String name) {
        if(name.equals("end"))
            result = pop();
        else
            throw new IllegalArgumentException("Machine code has wrong builin function :" + name);
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
