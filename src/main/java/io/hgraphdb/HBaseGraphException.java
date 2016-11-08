package io.hgraphdb;

public class HBaseGraphException extends RuntimeException {

    private static final long serialVersionUID = 8943901671174278248L;

    public HBaseGraphException() {
    }

    public HBaseGraphException(String reason) {
        super(reason);
    }

    public HBaseGraphException(Throwable cause) {
        super(cause);
    }

    public HBaseGraphException(String reason, Throwable cause) {
        super(reason, cause);
    }
}
