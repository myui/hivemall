package hivemall.smile.vm;

import javax.annotation.Nonnull;

/**
 * Created by narittan on 15/08/07.
 */
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
