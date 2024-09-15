package at.uibk.dps.dml.node.storage.command;

import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;

public class GetCommand extends InvalidationCommand {

    private static final InvalidationCommandType commandType = InvalidationCommandType.GET;

    public GetCommand() {
    }

    public GetCommand(Promise<Object> promise, String key, Integer lockToken, int originVerticleId,
                      boolean allowInvalidReads) {
        super(promise, key, lockToken, true, allowInvalidReads, originVerticleId, false);
    }

    @Override
    public Object apply(CommandHandler handler) {
        return handler.apply(this);
    }

    @Override
    public InvalidationCommandType getType() {
        return commandType;
    }

    @Override
    protected void encodePayload(Buffer buffer) {
        // Nothing to encode
    }

    @Override
    protected void decodePayload(BufferReader bufferReader) {
        // Nothing to decode
        readOnly = true;
    }
}
