package at.uibk.dps.dml.node.util;

import io.vertx.core.buffer.Buffer;

public class BufferReader {

    private final Buffer buffer;

    private int position;

    public BufferReader(Buffer buffer) {
        this.buffer = buffer;
    }

    public int getInt() {
        int value = buffer.getInt(position);
        position += 4;
        return value;
    }

    public long getLong() {
        long value = buffer.getLong(position);
        position += 8;
        return value;
    }

    public byte getByte() {
        byte value = buffer.getByte(position);
        position += 1;
        return value;
    }

    public byte[] getBytes(int length) {
        int oldPosition = position;
        position += length;
        return buffer.getBytes(oldPosition, position);
    }

    public byte[] getLengthPrefixedBytes() {
        int length = getInt();
        return length != -1 ? getBytes(length) : null;
    }

    public String getString(int length) {
        int oldPosition = position;
        position += length;
        return buffer.getString(oldPosition, position);
    }

    public String getLengthPrefixedString() {
        int length = getInt();
        return length != -1 ? getString(length) : null;
    }

    public boolean reachedEnd() {
        return position >= buffer.length();
    }
}
