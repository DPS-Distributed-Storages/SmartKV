package at.uibk.dps.dml.node.metadata.rpc;

import at.uibk.dps.dml.node.util.Timestamp;
import at.uibk.dps.dml.node.metadata.command.InvalidationCommand;
import at.uibk.dps.dml.node.util.ZoneInfo;
import io.vertx.core.Future;

public interface MetadataRpcService {

    Future<Void> invalidate(int remoteVerticleId, int epoch, Timestamp timestamp, InvalidationCommand command);

    Future<Void> commit(int remoteVerticleId, int epoch, String key, Timestamp timestamp);

    Future<ZoneInfo> getZoneInfo(int remoteVerticleId);

    Future<Integer> getFreeStorageNode(int remoteVerticleId, String zone, long objectSizeInBytes);
}
