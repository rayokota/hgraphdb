package io.hgraphdb;

public class HBaseGraphNotFoundException extends HBaseGraphException {

    public HBaseGraphNotFoundException() {
    }

    public HBaseGraphNotFoundException(String reason) {
        super(reason);
    }

    public HBaseGraphNotFoundException(Throwable cause) {
        super(cause);
    }

    public HBaseGraphNotFoundException(String reason, Throwable cause) {
        super(reason, cause);
    }
}
