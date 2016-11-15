package io.hgraphdb;

public class HBaseGraphNotUniqueException extends RuntimeException {

    private static final long serialVersionUID = 8943901671174278248L;

    public HBaseGraphNotUniqueException() {
    }

    public HBaseGraphNotUniqueException(String reason) {
        super(reason);
    }

    public HBaseGraphNotUniqueException(Throwable cause) {
        super(cause);
    }

    public HBaseGraphNotUniqueException(String reason, Throwable cause) {
        super(reason, cause);
    }
}
