package at.uibk.dps.dml.client.metadata.commands;

import at.uibk.dps.dml.client.Command;
import at.uibk.dps.dml.client.CommandType;
import at.uibk.dps.dml.client.metadata.MetadataCommandType;
import at.uibk.dps.dml.client.util.ReadableBuffer;
import io.vertx.core.buffer.Buffer;

import java.util.Set;

public class SynchronizedReconfigureCommand implements Command<Void> {

    private final String key;

    private final Set<Integer> seenReplicaNodeIds;

    private final Set<Integer> newReplicaNodeIds;

    public SynchronizedReconfigureCommand(String key, Set<Integer> seenReplicaNodeIds, Set<Integer> newReplicaNodeIds) {
        this.key = key;
        this.seenReplicaNodeIds = seenReplicaNodeIds;
        this.newReplicaNodeIds = newReplicaNodeIds;
    }

    @Override
    public CommandType getCommandType() {
        return MetadataCommandType.SYNCHRONIZED_RECONFIGURE;
    }

    @Override
    public void encode(Buffer buffer) {
        buffer.appendInt(key.length()).appendString(key);
        buffer.appendInt(seenReplicaNodeIds != null ? seenReplicaNodeIds.size() : -1);
        if (seenReplicaNodeIds != null) {
            for (int replicaNodeId : seenReplicaNodeIds) {
                buffer.appendInt(replicaNodeId);
            }
        }
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
