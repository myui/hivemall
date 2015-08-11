package hivemall.smile.vm;

public class VMRuntimeException extends Exception {
    private static final long serialVersionUID = -7378149197872357802L;

    public VMRuntimeException(String message) {
        super(message);
    }

    public VMRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

}
