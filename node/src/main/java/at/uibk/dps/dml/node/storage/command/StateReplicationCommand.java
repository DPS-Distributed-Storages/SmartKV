package at.uibk.dps.dml.node.storage.command;

import at.uibk.dps.dml.client.util.BufferUtil;
import at.uibk.dps.dml.node.util.BufferReader;
import at.uibk.dps.dml.node.storage.StorageObject;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import org.apache.commons.lang3.SerializationUtils;

public class StateReplicationCommand extends InvalidationCommand {

    private static final InvalidationCommandType commandType = InvalidationCommandType.STATE_REPLICATION;

    private StorageObject storageObject;

    public StateReplicationCommand() {
    }

    public StateReplicationCommand(Promise<Object> promise, String key, Integer lockToken,
                                   int originVerticleId, StorageObject storageObject) {
        super(promise, key, lockToken, false, false, originVerticleId, false);
        this.storageObject = storageObject;
    }

    public StorageObject getStorageObject() {
        return storageObject;
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
        BufferUtil.appendLengthPrefixedBytes(buffer, SerializationUtils.serialize(storageObject));
    }

    @Override
    protected void decodePayload(BufferReader bufferReader) {
        byte[] encodedObject = bufferReader.getLengthPrefixedBytes();
        storageObject = encodedObject != null ? SerializationUtils.deserialize(encodedObject) : null;
    }
}
