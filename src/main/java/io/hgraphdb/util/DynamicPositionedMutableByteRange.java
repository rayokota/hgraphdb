package io.hgraphdb.util;

import org.apache.hadoop.hbase.util.AbstractPositionedByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;

import java.util.Arrays;

public class DynamicPositionedMutableByteRange extends AbstractPositionedByteRange {
    /**
     * Create a new {@code PositionedByteRange} lacking a backing array and with
     * an undefined viewport.
     */
    public DynamicPositionedMutableByteRange() {
        super();
    }

    /**
     * Create a new {@code PositionedByteRange} over a new backing array of size
     * {@code capacity}. The range's offset and length are 0 and {@code capacity},
     * respectively.
     *
     * @param capacity the size of the backing array.
     */
    public DynamicPositionedMutableByteRange(int capacity) {
        this(new byte[capacity]);
    }

    /**
     * Create a new {@code PositionedByteRange} over the provided {@code bytes}.
     *
     * @param bytes The array to wrap.
     */
    public DynamicPositionedMutableByteRange(byte[] bytes) {
        set(bytes);
    }

    /**
     * Create a new {@code PositionedByteRange} over the provided {@code bytes}.
     *
     * @param bytes The array to wrap.
     * @param offset The offset into {@code bytes} considered the beginning of this
     *     range.
     * @param length The length of this range.
     */
    public DynamicPositionedMutableByteRange(byte[] bytes, int offset, int length) {
        set(bytes, offset, length);
    }

    @Override
    public PositionedByteRange unset() {
        this.position = 0;
        clearHashCache();
        bytes = null;
        offset = 0;
        length = 0;
        return this;
    }

    @Override
    public PositionedByteRange set(int capacity) {
        this.position = 0;
        super.set(capacity);
        this.limit = capacity;
        return this;
    }

    @Override
    public PositionedByteRange set(byte[] bytes) {
        this.position = 0;
        super.set(bytes);
        this.limit = bytes.length;
        return this;
    }

    @Override
    public PositionedByteRange set(byte[] bytes, int offset, int length) {
        this.position = 0;
        super.set(bytes, offset, length);
        limit = length;
        return this;
    }

    /**
     * Update the beginning of this range. {@code offset + length} may not be
     * greater than {@code bytes.length}. Resets {@code position} to 0.
     *
     * @param offset the new start of this range.
     * @return this.
     */
    @Override
    public PositionedByteRange setOffset(int offset) {
        this.position = 0;
        super.setOffset(offset);
        return this;
    }

    /**
     * Update the length of this range. {@code offset + length} should not be
     * greater than {@code bytes.length}. If {@code position} is greater than the
     * new {@code length}, sets {@code position} to {@code length}.
     *
     * @param length The new length of this range.
     * @return this.
     */
    @Override
    public PositionedByteRange setLength(int length) {
        this.position = Math.min(position, length);
        super.setLength(length);
        return this;
    }

    @Override
    public PositionedByteRange put(byte val) {
        ensureCapacity(position + 1);
        put(position++, val);
        return this;
    }

    @Override
    public PositionedByteRange put(byte[] val) {
        if (0 == val.length) {
            return this;
        }
        return this.put(val, 0, val.length);
    }

    @Override
    public PositionedByteRange put(byte[] val, int offset, int length) {
        if (0 == length) {
            return this;
        }
        ensureCapacity(position + length);
        put(position, val, offset, length);
        this.position += length;
        return this;
    }

    @Override
    public PositionedByteRange get(int index, byte[] dst) {
        super.get(index, dst);
        return this;
    }

    @Override
    public PositionedByteRange get(int index, byte[] dst, int offset, int length) {
        super.get(index, dst, offset, length);
        return this;
    }

    @Override
    public PositionedByteRange put(int index, byte val) {
        bytes[offset + index] = val;
        return this;
    }

    @Override
    public PositionedByteRange put(int index, byte[] val) {
        if (0 == val.length) {
            return this;
        }
        return put(index, val, 0, val.length);
    }

    @Override
    public PositionedByteRange put(int index, byte[] val, int offset, int length) {
        if (0 == length) {
            return this;
        }
        System.arraycopy(val, offset, this.bytes, this.offset + index, length);
        return this;
    }

    @Override
    public PositionedByteRange deepCopy() {
        DynamicPositionedMutableByteRange clone = new DynamicPositionedMutableByteRange(
            deepCopyToNewArray());
        clone.position = this.position;
        return clone;
    }

    @Override
    public PositionedByteRange shallowCopy() {
        DynamicPositionedMutableByteRange clone = new DynamicPositionedMutableByteRange(bytes,
            offset,
            length
        );
        clone.position = this.position;
        return clone;
    }

    @Override
    public PositionedByteRange shallowCopySubRange(int innerOffset, int copyLength) {
        DynamicPositionedMutableByteRange clone = new DynamicPositionedMutableByteRange(
            bytes,
            offset + innerOffset,
            copyLength
        );
        clone.position = this.position;
        return clone;
    }

    @Override
    public PositionedByteRange putShort(short val) {
        ensureCapacity(position + Bytes.SIZEOF_SHORT);
        putShort(position, val);
        position += Bytes.SIZEOF_SHORT;
        return this;
    }

    @Override
    public PositionedByteRange putInt(int val) {
        ensureCapacity(position + Bytes.SIZEOF_INT);
        putInt(position, val);
        position += Bytes.SIZEOF_INT;
        return this;
    }

    @Override
    public PositionedByteRange putLong(long val) {
        ensureCapacity(position + Bytes.SIZEOF_LONG);
        putLong(position, val);
        position += Bytes.SIZEOF_LONG;
        return this;
    }

    @Override
    public int putVLong(long val) {
        ensureCapacity(position + Bytes.SIZEOF_LONG);
        int len = putVLong(position, val);
        position += len;
        return len;
    }

    @Override
    public PositionedByteRange putShort(int index, short val) {
        // This writing is same as BB's putShort. When byte[] is wrapped in a BB and
        // call putShort(),
        // one can get the same result.
        bytes[offset + index + 1] = (byte) val;
        val >>= 8;
        bytes[offset + index] = (byte) val;
        clearHashCache();
        return this;
    }

    @Override
    public PositionedByteRange putInt(int index, int val) {
        // This writing is same as BB's putInt. When byte[] is wrapped in a BB and
        // call getInt(), one
        // can get the same result.
        for (int i = Bytes.SIZEOF_INT - 1; i > 0; i--) {
            bytes[offset + index + i] = (byte) val;
            val >>>= 8;
        }
        bytes[offset + index] = (byte) val;
        clearHashCache();
        return this;
    }

    @Override
    public PositionedByteRange putLong(int index, long val) {
        // This writing is same as BB's putLong. When byte[] is wrapped in a BB and
        // call putLong(), one
        // can get the same result.
        for (int i = Bytes.SIZEOF_LONG - 1; i > 0; i--) {
            bytes[offset + index + i] = (byte) val;
            val >>>= 8;
        }
        bytes[offset + index] = (byte) val;
        clearHashCache();
        return this;
    }

    // Copied from com.google.protobuf.CodedOutputStream v2.5.0 writeRawVarint64
    @Override
    public int putVLong(int index, long val) {
        int rPos = 0;
        while (true) {
            if ((val & ~0x7F) == 0) {
                bytes[offset + index + rPos] = (byte) val;
                break;
            } else {
                bytes[offset + index + rPos] = (byte) ((val & 0x7F) | 0x80);
                val >>>= 7;
            }
            rPos++;
        }
        clearHashCache();
        return rPos + 1;
    }
    // end copied from protobuf

    private void ensureCapacity(int minCapacity) {
        if (minCapacity - bytes.length > 0) {
            grow(minCapacity);
        }
    }

    private void grow(int minCapacity) {
        int oldCapacity = bytes.length;
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0) {
            newCapacity = minCapacity;
        }
        byte[] newBytes = Arrays.copyOf(bytes, newCapacity);
        int oldPosition = position;
        super.set(newBytes);
        this.position = oldPosition;
        this.limit = newBytes.length;
    }
}
