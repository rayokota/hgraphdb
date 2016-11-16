package io.hgraphdb;

public class HBaseGraphNotValidException extends RuntimeException {

    private static final long serialVersionUID = 8943901671174278248L;

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
