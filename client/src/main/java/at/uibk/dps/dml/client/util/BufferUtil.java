package at.uibk.dps.dml.client.util;

import io.netty.util.CharsetUtil;
import io.vertx.core.buffer.Buffer;

public final class BufferUtil {

    private BufferUtil() {
    }

    public static void appendLengthPrefixedBytes(Buffer buffer, byte[] bytes) {
        if (bytes == null) {
            buffer.appendInt(-1);
            return;
        }
        buffer.appendInt(bytes.length).appendBytes(bytes);
    }

    public static void appendLengthPrefixedString(Buffer buffer, String string) {
        if (string == null) {
            buffer.appendInt(-1);
            return;
        }
        appendLengthPrefixedBytes(buffer, string.getBytes(CharsetUtil.UTF_8));
    }
}
