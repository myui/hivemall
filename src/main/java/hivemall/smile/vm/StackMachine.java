package hivemall.smile.vm;

import java.util.Stack;
import java.util.*;
import java.lang.*;

public class StackMachine {

    private HashMap<String, Double> valuesMap;
    private HashMap<String, Integer> jumpMap;
    private ArrayList<Operation> code = new ArrayList<Operation>();
    private Stack<Double> programStack;
    private Integer IP;
    private Integer SP = 0;
    private Integer codeLength;
    private boolean[] done;
    private double result = 0;

    public void run(ArrayList<String> script, double[] features){
        for (String line:script){
            String[] ops = line.split(" ", -1);
            if (ops.length==2){
                Operation.OperationEnum o = Operation.OperationEnum.valueOfLowerCase(ops[0]);
                code.add(new Operation(o, ops[1]));
            }
            else{
                Operation.OperationEnum o = Operation.OperationEnum.valueOfLowerCase(ops[0]);
                code.add(new Operation(o));
            }
        }

        valuesMap = new HashMap<String, Double>();
        jumpMap = new HashMap<String, Integer>();
        codeLength = script.size()-1;
        done = new boolean[codeLength+1];

        bind(features);
        execute(0);
    }

    public void execute(int entryPoint) {
        valuesMap.put("end", -1.0);
        jumpMap.put("last", codeLength);

        IP = entryPoint;
        programStack = new Stack<Double>();
        while (IP<code.size()) {
            if (done[IP]) {
                throw new IllegalArgumentException("There is a infinite loop in the Machine code.");
            }
            done[IP] = true;
            Operation currentOperation = code.get((int) IP);
            if (!executeOperation(currentOperation)) {
                return;
            }
        }
    }

    public void bind(double[] features) {
        for (int i=0; i<features.length; i++){
            StringBuilder bindKey = new StringBuilder();
            bindKey.append("x[").append(String.valueOf(i)).append("]");
            valuesMap.put(bindKey.toString(), features[i]);
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

    private boolean executeOperation(Operation currentOperation) {
        if (IP < 0)
            return false;
        switch (currentOperation.op) {
            case GOTO:
                if (isInt(currentOperation.operand))
                    IP = Integer.parseInt(currentOperation.operand);
                else
                    IP = jumpMap.get(currentOperation.operand);
                break;
            case CALL:
                double candidateIP = valuesMap.get(currentOperation.operand);
                if (candidateIP < 0) {
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
                if (a == b || Math.abs(a - b) <= Math.min(absa, absb) * 2.2204460492503131e-16)
                    if (isInt(currentOperation.operand))
                        IP = Integer.parseInt(currentOperation.operand);
                    else
                        IP = jumpMap.get(currentOperation.operand);
                else
                    IP++;
                break;
            case IFGR:
                double lower = pop();
                double upper = pop();
                if (upper > lower)
                    if (isInt(currentOperation.operand))
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
                if (isDouble(currentOperation.operand))
                    push(Double.parseDouble(currentOperation.operand));
                else
                    push(valuesMap.get(currentOperation.operand));
                IP++;
                break;
            default:
                throw new IllegalArgumentException("Machine code has wrong opcode :" + currentOperation.op);
        }
        return true;

    }

    private void evaluateBuiltinByName(String name) {
        if (name.equals("end"))
            result = pop();
        else
            throw new IllegalArgumentException("Machine code has wrong builin function :" + name);
    }

    public boolean isInt(String i) {
        try {
            Integer.parseInt(i);
            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }

    public boolean isDouble(String i) {
        try {
            Double.parseDouble(i);
            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }

}
