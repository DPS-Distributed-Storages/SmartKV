package at.uibk.dps.dml.node.storage.rpc;

import at.uibk.dps.dml.node.util.Timestamp;
import at.uibk.dps.dml.node.storage.StorageObject;
import at.uibk.dps.dml.node.storage.command.InvalidationCommand;
import io.vertx.core.Future;

/**
 * The {@link StorageRpcService} is used to communicate with remote storage verticles.
 */
public interface StorageRpcService {

    /**
     * Sends an invalidation command to the remote storage verticle.
     *
     * @param remoteVerticleId the ID of the remote storage verticle
     * @param timestamp the timestamp of the invalidation command
     * @param command the invalidation command
     * @return a future that completes when the invalidation command has been handled
     */
    Future<Void> invalidate(int remoteVerticleId, int epoch, Timestamp timestamp, InvalidationCommand command);

    /**
     * Sends a commit request for the given key to the remote storage verticle.
     *
     * @param remoteVerticleId the ID of the remote storage verticle
     * @param key the key of the object to commit
     * @param timestamp the timestamp of the corresponding invalidation command
     * @return a future that completes when the commit request has been handled
     */
    Future<Void> commit(int remoteVerticleId, int epoch, String key, Timestamp timestamp);

    /**
     * Requests the object with the given key from the remote storage verticle.
     *
     * @param remoteVerticleId the ID of the remote storage verticle
     * @param key the key of the object to request
     * @return a future that completes with the requested object
     */
    Future<StorageObject> getObject(int remoteVerticleId, int epoch, String key);


    /**
     * Requests the free memory from the storage verticle.
     *
     * @param remoteVerticleId the ID of the remote storage verticle
     * @param epoch the current epoch
     * @return a future that completes with the requested object
     */
    Future<Long> getFreeMemory(int remoteVerticleId, int epoch);

}
