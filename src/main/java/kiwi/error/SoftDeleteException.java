package kiwi.error;

public class SoftDeleteException extends KiwiException {
    public SoftDeleteException(String message, Throwable cause) {
        super(message, cause);
    }
}
