package io.hgraphdb;

public class HBaseGraphNotUniqueException extends HBaseGraphException {

    private static final long serialVersionUID = 4805483627434605597L;

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
