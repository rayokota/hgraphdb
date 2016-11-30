package io.hgraphdb;

public class HBaseGraphNoSchemaException extends RuntimeException {

    private static final long serialVersionUID = 8943901671174278248L;

    public HBaseGraphNoSchemaException() {
    }

    public HBaseGraphNoSchemaException(String reason) {
        super(reason);
    }

    public HBaseGraphNoSchemaException(Throwable cause) {
        super(cause);
    }

    public HBaseGraphNoSchemaException(String reason, Throwable cause) {
        super(reason, cause);
    }
}
