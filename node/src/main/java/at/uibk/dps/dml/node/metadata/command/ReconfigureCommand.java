package at.uibk.dps.dml.node.metadata.command;

import at.uibk.dps.dml.node.metadata.KeyMetadata;
import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;

public class ReconfigureCommand extends InvalidationCommand {

    private static final InvalidationCommandType commandType = InvalidationCommandType.RECONFIGURE;

    private KeyMetadata newMetadata;

    public ReconfigureCommand() {
    }

    public ReconfigureCommand(Promise<Object> promise, String key, KeyMetadata newMetadata) {
        super(promise, key);
        this.newMetadata = newMetadata;
    }

    public KeyMetadata getNewMetadata() {
        return newMetadata;
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
        encodeMetadata(buffer, newMetadata);
    }

    @Override
    protected void decodePayload(BufferReader bufferReader) {
        newMetadata = decodeMetadata(bufferReader);
    }
}
