package hivemall.smile.vm;

/**
 * Created by narittan on 15/08/07.
 */
public class Operation {

    OperationEnum op;
    String operand;

    Operation(OperationEnum op, String operand) {
        this.op = op;
        this.operand = operand;
    }

    Operation(OperationEnum op) {
        this.op = op;
        this.operand = null;
    }

    public enum OperationEnum {
        // FUNCTION is pseudo operation. it will never be executed and don't have IP itself.
        FUNCTION,PUSH, POP, DUP, ADD, SUB, DIV, MUL, GOTO, IFEQ, IFGR, CALL;
        static OperationEnum valueOfLowerCase(String op) {
            return OperationEnum.valueOf(op.toUpperCase());
        }
    }

    @Override
    public String toString() {
        return op.toString() + (operand != null ? (" " + operand) : "");
    }

}
