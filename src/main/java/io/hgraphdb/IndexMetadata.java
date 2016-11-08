package io.hgraphdb;

public class IndexMetadata {

    private final Key key;
    private State state;
    protected Long createdAt;
    protected Long updatedAt;

    public IndexMetadata(IndexType type, String label, String propertyKey, State state, Long createdAt, Long updatedAt) {
        this.key = new Key(type, label, propertyKey);
        this.state = state;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public Key key() {
        return key;
    }

    public IndexType type() {
        return key.type();
    }

    public String label() {
        return key.label();
    }

    public String propertyKey() {
        return key.propertyKey();
    }

    public State state() {
        return state;
    }

    public void state(State state) {
        this.state = state;
    }

    public Long createdAt() {
        return createdAt;
    }

    public Long updatedAt() {
        return updatedAt;
    }

    public void updatedAt(Long updatedAt) {
        this.updatedAt = updatedAt;
    }

    @Override
    public String toString() {
        return key.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexMetadata that = (IndexMetadata) o;

        return key.equals(that.key);

    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    public static class Key {
        private final IndexType type;
        private final String label;
        private final String propertyKey;

        public Key(IndexType type, String label, String propertyKey) {
            this.type = type;
            this.label = label;
            this.propertyKey = propertyKey;
        }

        public IndexType type() {
            return type;
        }

        public String label() {
            return label;
        }

        public String propertyKey() {
            return propertyKey;
        }

        @Override
        public String toString() {
            return "INDEX " + type + ":" + label + "(" + propertyKey() + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key key = (Key) o;

            if (type != key.type) return false;
            if (!label.equals(key.label)) return false;
            return propertyKey.equals(key.propertyKey);
        }

        @Override
        public int hashCode() {
            int result = type.hashCode();
            result = 31 * result + label.hashCode();
            result = 31 * result + propertyKey.hashCode();
            return result;
        }
    }

    public enum State {
        CREATED,
        BUILDING,
        ACTIVE,
        INACTIVE, // an intermediate state to wait for clients to stop using an index
        DROPPED
    }
}
