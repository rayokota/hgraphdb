package io.hgraphdb;

public class HBaseGraphNotValidException extends HBaseGraphException {

    private static final long serialVersionUID = -8239184421076563214L;

    public HBaseGraphNotValidException() {
    }

    public HBaseGraphNotValidException(String reason) {
        super(reason);
    }

    public HBaseGraphNotValidException(Throwable cause) {
        super(cause);
    }

    public HBaseGraphNotValidException(String reason, Throwable cause) {
        super(reason, cause);
    }
}
