package at.uibk.dps.dml.node.metadata.command;

import at.uibk.dps.dml.node.metadata.KeyMetadata;
import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;

public class CreateCommand extends InvalidationCommand {

    private static final InvalidationCommandType commandType = InvalidationCommandType.CREATE;

    private KeyMetadata metadata;

    public CreateCommand() {
    }

    public CreateCommand(Promise<Object> promise, String key, KeyMetadata metadata) {
        super(promise, key);
        this.metadata = metadata;
    }

    public KeyMetadata getMetadata() {
        return metadata;
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
        encodeMetadata(buffer, metadata);
    }

    @Override
    protected void decodePayload(BufferReader bufferReader) {
        metadata = decodeMetadata(bufferReader);
    }
}
