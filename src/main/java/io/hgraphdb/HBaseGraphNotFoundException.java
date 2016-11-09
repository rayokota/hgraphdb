package io.hgraphdb;

import org.apache.tinkerpop.gremlin.structure.Element;

public class HBaseGraphNotFoundException extends HBaseGraphException {

    private HBaseElement element;

    public HBaseGraphNotFoundException(Element element) {
        this.element = (HBaseElement) element;
    }

    public HBaseGraphNotFoundException(Element element, String reason) {
        super(reason);
        this.element = (HBaseElement) element;
    }

    public HBaseGraphNotFoundException(Element element, Throwable cause) {
        super(cause);
        this.element = (HBaseElement) element;
    }

    public HBaseGraphNotFoundException(Element element, String reason, Throwable cause) {
        super(reason, cause);
        this.element = (HBaseElement) element;
    }

    public HBaseElement getElement() {
        return element;
    }
}
