package at.uibk.dps.dml.node.metadata.command;

import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;

public class DeleteCommand extends InvalidationCommand {

    private static final InvalidationCommandType commandType = InvalidationCommandType.DELETE;

    public DeleteCommand() {
    }

    public DeleteCommand(Promise<Object> promise, String key) {
        super(promise, key);
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
    }
}
