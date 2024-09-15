package at.uibk.dps.dml.client.metadata.commands;

import at.uibk.dps.dml.client.Command;
import at.uibk.dps.dml.client.CommandType;
import at.uibk.dps.dml.client.metadata.MetadataCommandType;
import at.uibk.dps.dml.client.util.ReadableBuffer;
import io.vertx.core.buffer.Buffer;

import java.util.*;

public class ReconfigureCommand implements Command<Void> {

    private final String key;

    private final Set<Integer> newReplicaNodeIds;

    public ReconfigureCommand(String key, Set<Integer> newReplicaNodeIds) {
        this.key = key;
        this.newReplicaNodeIds = newReplicaNodeIds;
    }

    @Override
    public CommandType getCommandType() {
        return MetadataCommandType.RECONFIGURE;
    }

    @Override
    public void encode(Buffer buffer) {
        buffer.appendInt(key.length()).appendString(key);
        buffer.appendInt(newReplicaNodeIds != null ? newReplicaNodeIds.size() : -1);
        if (newReplicaNodeIds != null) {
            for (int replicaNodeId : newReplicaNodeIds) {
                buffer.appendInt(replicaNodeId);
            }
        }
    }

    @Override
    public Void decodeReply(ReadableBuffer buffer) {
        return null;
    }
}
