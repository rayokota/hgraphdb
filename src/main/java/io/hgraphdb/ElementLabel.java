package io.hgraphdb;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class ElementLabel {

    private String label;
    private ValueType idType;
    private Map<String, ValueType> propertyTypes;

    public ElementLabel(String label, ValueType idType, Object... propertyKeysAndTypes) {
        this.label = label;
        this.idType = idType;
        this.propertyTypes = HBaseGraphUtils.propertyKeysAndTypesToMap(propertyKeysAndTypes);
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
