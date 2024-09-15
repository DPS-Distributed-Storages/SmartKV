package at.uibk.dps.dml.node.storage.rpc;

import at.uibk.dps.dml.node.RpcType;
import at.uibk.dps.dml.node.billing.BillingService;
import at.uibk.dps.dml.node.util.Timestamp;
import at.uibk.dps.dml.node.storage.StorageObject;
import at.uibk.dps.dml.node.storage.command.InvalidationCommand;
import at.uibk.dps.dml.node.exception.StorageRpcException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the {@link StorageRpcService} interface that communicates with remote storage verticles over
 * the Vert.x event bus.
 */
public class StorageRpcServiceImpl implements StorageRpcService {

    private final Logger logger = LoggerFactory.getLogger(StorageRpcServiceImpl.class);

    private final Vertx vertx;

    private final int verticleId;

    private BillingService billingService;

    /**
     * Default constructor.
     *
     * @param vertx the Vert.x instance
     * @param verticleId the ID of the storage verticle (origin)
     */
    public StorageRpcServiceImpl(Vertx vertx, int verticleId, BillingService billingService) {
        this.vertx = vertx;
        this.verticleId = verticleId;
        this.billingService = billingService;
    }

    @Override
    public Future<Void> invalidate(int remoteVerticleId, int epoch, Timestamp timestamp, InvalidationCommand command) {
        Buffer buffer = Buffer.buffer();
        buffer.appendByte((byte) RpcType.STORAGE.ordinal())
                .appendByte((byte) StorageRpcType.INVALIDATE.ordinal())
                .appendInt(verticleId)
                .appendInt(epoch)
                .appendLong(timestamp.getVersion())
                .appendInt(timestamp.getCoordinatorVerticleId())
                .appendByte((byte) command.getType().ordinal());

        command.encode(buffer);
        billingService.addRpcDataTransferOut(buffer.length(), remoteVerticleId);
        return vertx.eventBus()
                .<Void>request(String.valueOf(remoteVerticleId), buffer)
                .transform(res -> transformRequestResult(res, remoteVerticleId));
    }

    @Override
    public Future<Void> commit(int remoteVerticleId, int epoch, String key, Timestamp timestamp) {
        Buffer buffer = Buffer.buffer();
        buffer.appendByte((byte) RpcType.STORAGE.ordinal())
                .appendByte((byte) StorageRpcType.COMMIT.ordinal())
                .appendInt(verticleId)
                .appendInt(epoch)
                .appendInt(key.length())
                .appendString(key)
                .appendLong(timestamp.getVersion())
                .appendInt(timestamp.getCoordinatorVerticleId());
        billingService.addRpcDataTransferOut(buffer.length(), remoteVerticleId);
        return vertx.eventBus()
                .<Void>request(String.valueOf(remoteVerticleId), buffer)
                .transform(res -> transformRequestResult(res, remoteVerticleId));
    }

    @Override
    public Future<StorageObject> getObject(int remoteVerticleId, int epoch, String key) {
        Buffer buffer = Buffer.buffer();
        buffer.appendByte((byte) RpcType.STORAGE.ordinal())
                .appendByte((byte) StorageRpcType.GET_OBJECT.ordinal())
                .appendInt(verticleId)
                .appendInt(epoch)
                .appendInt(key.length())
                .appendString(key);
        billingService.addRpcDataTransferOut(buffer.length(), remoteVerticleId);
        return vertx.eventBus()
                .<Buffer>request(String.valueOf(remoteVerticleId), buffer)
                .transform(res -> {
                    return transformRequestResult(res, remoteVerticleId);
                })
                .map(replyBuffer -> {
                    billingService.addRpcDataTransferIn(buffer.length(), remoteVerticleId);
                    return SerializationUtils.deserialize(replyBuffer.getBytes());
                });
    }

    @Override
    public Future<Long> getFreeMemory(int remoteVerticleId, int epoch) {
        Buffer buffer = Buffer.buffer();
        buffer.appendByte((byte) RpcType.STORAGE.ordinal())
                .appendByte((byte) StorageRpcType.GET_FREE_MEMORY.ordinal())
                .appendInt(verticleId)
                .appendInt(epoch);
        billingService.addRpcDataTransferOut(buffer.length(), remoteVerticleId);
        return vertx.eventBus()
                .<Buffer>request(String.valueOf(remoteVerticleId), buffer)
                .transform(res -> {
                    return transformRequestResult(res, remoteVerticleId);
                })
                .map(replyBuffer -> {
                    billingService.addRpcDataTransferIn(buffer.length(), remoteVerticleId);
                    return SerializationUtils.deserialize(replyBuffer.getBytes());
                });
    }

    private <T> Future<T> transformRequestResult(AsyncResult<Message<T>> res, int remoteVerticleId) {
        // The message body will be always empty here for invalidate and commit except for a succeeded future
        // Thus we neglect egress data transfer in the monitoring service
        if (res.succeeded()) {
            // Return the body of the message
            return Future.succeededFuture(res.result().body());
        } else {
            logger.error("Storage RPC failed", res.cause());

            // Transform the ReplyException to a StorageRpcException
            ReplyException replyException = (ReplyException) res.cause();
            StorageRpcErrorType errorType;
            switch (replyException.failureType()) {
                case RECIPIENT_FAILURE:
                    errorType = StorageRpcErrorType.values()[replyException.failureCode()];
                    break;
                case TIMEOUT:
                    errorType = StorageRpcErrorType.TIMEOUT;
                    break;
                case NO_HANDLERS:
                default:
                    errorType = StorageRpcErrorType.UNKNOWN_ERROR;
                    break;
            }
            StorageRpcException exception = new StorageRpcException(errorType, replyException.getMessage());
            billingService.addRpcDataTransferOut(SerializationUtils.serialize(exception).length, remoteVerticleId);
            return Future.failedFuture(exception);
        }
    }
}

