package io.hgraphdb;

import java.util.Map;

public class LabelMetadata {

    private final Key key;
    private ValueType idType;
    private Map<String, ValueType> propertyTypes;
    protected Long createdAt;

    public LabelMetadata(ElementType type, String label, ValueType idType, Long createdAt, Map<String, ValueType> propertyTypes) {
        this.key = new Key(type, label);
        this.idType = idType;
        this.propertyTypes = propertyTypes;
        this.createdAt = createdAt;
    }

    public Key key() {
        return key;
    }

    public ElementType type() {
        return key.type();
    }

    public String label() {
        return key.label();
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
    public String toString() {
        return key.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LabelMetadata that = (LabelMetadata) o;

        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    public static class Key {
        private final ElementType type;
        private final String label;

        public Key(ElementType type, String label) {
            this.type = type;
            this.label = label;
        }

        public ElementType type() {
            return type;
        }

        public String label() {
            return label;
        }

        @Override
        public String toString() {
            return type + " LABEL " + " " + label;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key key = (Key) o;

            if (type != key.type) return false;
            return (label.equals(key.label));
        }

        @Override
        public int hashCode() {
            int result = type.hashCode();
            result = 31 * result + label.hashCode();
            return result;
        }
    }
}
