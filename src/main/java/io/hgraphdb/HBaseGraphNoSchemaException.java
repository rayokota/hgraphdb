package io.hgraphdb;

public class HBaseGraphNoSchemaException extends HBaseGraphException {

    private static final long serialVersionUID = 8959506127869603126L;

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
