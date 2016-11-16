package io.hgraphdb;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class ElementLabel {

    private String label;
    private ValueType idType;
    private Integer ttl;
    private Map<String, ValueType> propertyKeys;

    public ElementLabel(String label, ValueType idType, Integer ttl, Object... propertyKeysAndTypes) {
        this.label = label;
        this.idType = idType;
        this.ttl = ttl;
        this.propertyKeys = HBaseGraphUtils.propertyKeysAndTypesToMap(propertyKeysAndTypes);
    }

    public String label() {
        return label;
    }

    public ValueType idType() {
        return idType;
    }

    public int ttl() {
        return ttl;
    }

    public List<String> propertyKeys() {
        return propertyKeys();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ElementLabel that = (ElementLabel) o;

        return label.equals(that.label);

    }

    @Override
    public int hashCode() {
        return label.hashCode();
    }
}
