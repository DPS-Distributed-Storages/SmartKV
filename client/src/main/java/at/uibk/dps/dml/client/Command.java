package at.uibk.dps.dml.client;

import at.uibk.dps.dml.client.util.ReadableBuffer;
import io.vertx.core.buffer.Buffer;

public interface Command<R> {

    CommandType getCommandType();

    void encode(Buffer buffer);

    R decodeReply(ReadableBuffer buffer);

}
