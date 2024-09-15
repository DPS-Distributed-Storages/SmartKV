package at.uibk.dps.dml.client.metadata;

import at.uibk.dps.dml.client.BaseTcpClient;
import at.uibk.dps.dml.client.metadata.commands.*;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.*;

public class MetadataClient extends BaseTcpClient {

    public MetadataClient(Vertx vertx) {
        super(vertx);
    }



    public Future<Void> create(String key) {
        return create(key, null, false);
    }
    public Future<Void> create(String key, Set<Integer> replicaNodeIds) {
        return create(key, replicaNodeIds, false);
    }
    public Future<Void> createFullyReplicated(String key) {
        return create(key, null, true);
    }
    public Future<Void> create(String key, Set<Integer> replicaNodeIds, boolean fullReplication) {
        if (replicaNodeIds != null && replicaNodeIds.isEmpty() && !fullReplication) {
            throw new IllegalArgumentException();
        }
        return request(new CreateCommand(key, replicaNodeIds, fullReplication));
    }

    public Future<KeyConfiguration> get(String key) {
        return request(new GetCommand(key));
    }

    public Future<Map<String, KeyConfiguration>> getAll() {
        return request(new GetAllCommand());
    }

    public Future<Void> delete(String key) {
        return request(new DeleteCommand(key));
    }

    public Future<Void> reconfigure(String key, Set<Integer> newReplicaNodeIds) {
        if (newReplicaNodeIds == null || newReplicaNodeIds.isEmpty()) {
            throw new IllegalArgumentException();
        }
        return request(new ReconfigureCommand(key, newReplicaNodeIds));
    }

    public Future<Void> synchronizedReconfigure(String key, Set<Integer> seenReplicaNodeIds, Set<Integer> newReplicaNodeIds) {
        if (newReplicaNodeIds == null || newReplicaNodeIds.isEmpty()) {
            throw new IllegalArgumentException();
        }
        return request(new SynchronizedReconfigureCommand(key, seenReplicaNodeIds, newReplicaNodeIds));
    }

    public Future<String> getMembershipView() {
        return request(new GetMembershipViewCommand());
    }

    public Future<String> getZoneInfo(String zone) {
        return request(new GetZoneInfoCommand(zone));
    }

    public Future<String> getFreeStorageNodes(Set<String> zones, long objectSizeInBytes) {
        return request(new GetFreeStorageNodeCommand(zones, objectSizeInBytes));
    }

    @Override
    protected MetadataCommandError decodeCommandResultError(int errorCode, String message) {
        return new MetadataCommandError(MetadataCommandErrorType.valueOf(errorCode));
    }
}
