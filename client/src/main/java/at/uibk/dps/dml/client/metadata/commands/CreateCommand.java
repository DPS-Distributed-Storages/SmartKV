package at.uibk.dps.dml.client.metadata.commands;

import at.uibk.dps.dml.client.Command;
import at.uibk.dps.dml.client.CommandType;
import at.uibk.dps.dml.client.metadata.MetadataCommandType;
import at.uibk.dps.dml.client.util.ReadableBuffer;
import io.vertx.core.buffer.Buffer;

import java.util.Set;

public class CreateCommand implements Command<Void> {

    private final String key;

    private final Set<Integer> replicaNodeIds;

    private final boolean fullReplication;

    public CreateCommand(String key, Set<Integer> replicaNodeIds) {
        this.key = key;
        this.replicaNodeIds = replicaNodeIds;
        this.fullReplication = false;
    }

    public CreateCommand(String key, Set<Integer> replicaNodeIds, boolean fullReplication) {
        this.key = key;
        this.replicaNodeIds = replicaNodeIds;
        this.fullReplication = fullReplication;
    }

    @Override
    public CommandType getCommandType() {
        return MetadataCommandType.CREATE;
    }

    @Override
    public void encode(Buffer buffer) {
        buffer.appendInt(key.length()).appendString(key);
        buffer.appendInt(fullReplication ? 1 : 0);
        buffer.appendInt(replicaNodeIds != null ? replicaNodeIds.size() : -1);
        if (replicaNodeIds != null) {
            for (int replicaNodeId : replicaNodeIds) {
                buffer.appendInt(replicaNodeId);
            }
        }
    }

    @Override
    public Void decodeReply(ReadableBuffer buffer) {
        return null;
    }
}
