package at.uibk.dps.dml.node.metadata.command;

import at.uibk.dps.dml.node.metadata.KeyMetadata;
import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;

/**
 * If multiple optimizers could optimize the same key in parallel, the simple {@link ReconfigureCommand} is not sufficient,
 * as this could result in a race condition of the KeyMetadata. I.e. one optimizer might override the decisions of another optimizer
 * operating at the same time. This is resolved by this version of the reconfigure command.
 */
public class SynchronizedReconfigureCommand extends InvalidationCommand {

    private static final InvalidationCommandType commandType = InvalidationCommandType.SYNCHRONIZED_RECONFIGURE;

    private KeyMetadata newMetadata;

    private KeyMetadata seenMetadata;

    public SynchronizedReconfigureCommand() {
    }

    public SynchronizedReconfigureCommand(Promise<Object> promise, String key, KeyMetadata seenMetadata, KeyMetadata newMetadata) {
        super(promise, key);
        this.seenMetadata = seenMetadata;
        this.newMetadata = newMetadata;
    }

    public void setNewMetadata(KeyMetadata newMetadata) {
        this.newMetadata = newMetadata;
    }

    public void setSeenMetadata(KeyMetadata seenMetadata) {
        this.seenMetadata = seenMetadata;
    }

    public KeyMetadata getNewMetadata() {
        return this.newMetadata;
    }

    public KeyMetadata getSeenMetadata() {
        return this.seenMetadata;
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
        encodeMetadata(buffer, seenMetadata);
        encodeMetadata(buffer, newMetadata);
    }

    @Override
    protected void decodePayload(BufferReader bufferReader) {
        seenMetadata = decodeMetadata(bufferReader);
        newMetadata = decodeMetadata(bufferReader);
    }
}
