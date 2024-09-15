package at.uibk.dps.dml.client.util;

import io.vertx.core.buffer.Buffer;

public class ReadableBuffer {

    private final Buffer buffer;

    private int position;

    public ReadableBuffer(Buffer buffer) {
        this.buffer = buffer;
    }

    public boolean isEnd() {
        return position >= buffer.length();
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

    public String getString(int length) {
        int oldPosition = position;
        position += length;
        return buffer.getString(oldPosition, position);
    }

    public String getLengthPrefixedString() {
        int length = getInt();
        return length != -1 ? getString(length) : null;
    }

    public int getPosition() {
        return position;
    }
}
