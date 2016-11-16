package io.hgraphdb;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class ElementLabel {

    private String label;
    private ValueType idType;
    private Map<String, ValueType> propertyTypes;
    protected Long createdAt;

    public ElementLabel(String label, ValueType idType, Long createdAt, Map<String, ValueType> propertyTypes) {
        this.label = label;
        this.idType = idType;
        this.propertyTypes = propertyTypes;
        this.createdAt = createdAt;
    }

    public String label() {
        return label;
    }

    public ValueType idType() {
        return idType;
    }

    public Map<String, ValueType> propertyTypes() {
        return propertyTypes;
    }

    public Long createdAt() {
        return createdAt;
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
