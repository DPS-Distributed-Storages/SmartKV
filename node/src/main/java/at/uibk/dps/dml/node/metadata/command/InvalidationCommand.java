package at.uibk.dps.dml.node.metadata.command;

import at.uibk.dps.dml.client.util.BufferUtil;
import at.uibk.dps.dml.node.metadata.KeyMetadata;
import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;

import java.util.LinkedHashSet;
import java.util.Set;

public abstract class InvalidationCommand extends Command {

    protected KeyMetadata oldMetadata;

    protected InvalidationCommand() {
    }

    protected InvalidationCommand(Promise<Object> promise, String key) {
        super(promise, key, false, false);
    }

    public abstract InvalidationCommandType getType();

    public KeyMetadata getOldMetadata() {
        return oldMetadata;
    }

    public void setOldMetadata(KeyMetadata oldMetadata) {
        this.oldMetadata = oldMetadata;
    }

    public final void encode(Buffer buffer) {
        BufferUtil.appendLengthPrefixedString(buffer, key);
        encodeMetadata(buffer, oldMetadata);
        encodePayload(buffer);
    }

    public final void decode(BufferReader bufferReader) {
        key = bufferReader.getLengthPrefixedString();
        oldMetadata = decodeMetadata(bufferReader);
        decodePayload(bufferReader);
    }

    protected abstract void encodePayload(Buffer buffer);

    protected abstract void decodePayload(BufferReader buffer);

    protected void encodeMetadata(Buffer buffer, KeyMetadata metadata) {
        if (metadata == null) {
            buffer.appendInt(-1);
            return;
        }
        buffer.appendInt(metadata.isFullReplication() ? 1 : 0);
        buffer.appendInt(metadata.getObjectLocations().size());
        for (int verticleId : metadata.getObjectLocations()) {
            buffer.appendInt(verticleId);
        }
    }

    protected KeyMetadata decodeMetadata(BufferReader bufferReader) {
        int fullReplicationInt = bufferReader.getInt();
        if (fullReplicationInt == -1) {
            return null;
        }
        boolean fullReplication = fullReplicationInt == 1 ? true : false;
        int numberOfLocations = bufferReader.getInt();
        Set<Integer> objectLocations = new LinkedHashSet<>(numberOfLocations);
        for (int i = 0; i < numberOfLocations; i++) {
            objectLocations.add(bufferReader.getInt());
        }
        return new KeyMetadata(objectLocations, fullReplication);
    }
}
