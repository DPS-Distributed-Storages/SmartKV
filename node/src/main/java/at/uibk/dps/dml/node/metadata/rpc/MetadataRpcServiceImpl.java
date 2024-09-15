package at.uibk.dps.dml.node.metadata.rpc;

import at.uibk.dps.dml.node.RpcType;
import at.uibk.dps.dml.node.util.Timestamp;
import at.uibk.dps.dml.node.exception.MetadataRpcException;
import at.uibk.dps.dml.node.metadata.command.InvalidationCommand;
import at.uibk.dps.dml.node.util.ZoneInfo;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataRpcServiceImpl implements MetadataRpcService {

    private final Logger logger = LoggerFactory.getLogger(MetadataRpcServiceImpl.class);

    private final Vertx vertx;

    public MetadataRpcServiceImpl(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public Future<Void> invalidate(int remoteVerticleId, int epoch, Timestamp timestamp, InvalidationCommand command) {
        Buffer buffer = Buffer.buffer();
        buffer.appendByte((byte) RpcType.METADATA.ordinal())
                .appendByte((byte) MetadataRpcType.INVALIDATE.ordinal())
                .appendInt(epoch)
                .appendLong(timestamp.getVersion())
                .appendInt(timestamp.getCoordinatorVerticleId())
                .appendByte((byte) command.getType().ordinal());

        command.encode(buffer);

        return vertx.eventBus()
                .<Void>request(String.valueOf(remoteVerticleId), buffer)
                .transform(this::transformRequestResult);
    }

    @Override
    public Future<Void> commit(int remoteVerticleId, int epoch, String key, Timestamp timestamp) {
        Buffer buffer = Buffer.buffer();
        buffer.appendByte((byte) RpcType.METADATA.ordinal())
                .appendByte((byte) MetadataRpcType.COMMIT.ordinal())
                .appendInt(epoch)
                .appendInt(key.length())
                .appendString(key)
                .appendLong(timestamp.getVersion())
                .appendInt(timestamp.getCoordinatorVerticleId());
        return vertx.eventBus()
                .<Void>request(String.valueOf(remoteVerticleId), buffer)
                .transform(this::transformRequestResult);
    }

    @Override
    public Future<ZoneInfo> getZoneInfo(int remoteVerticleId) {
        Buffer buffer = Buffer.buffer();
        buffer.appendByte((byte) RpcType.METADATA.ordinal())
                .appendByte((byte) MetadataRpcType.GET_ZONE_INFO.ordinal());
        return vertx.eventBus()
                .<Buffer>request(String.valueOf(remoteVerticleId), buffer)
                .transform(this::transformRequestResult)
                .map(replyBuffer -> {
                    return SerializationUtils.deserialize(replyBuffer.getBytes());
                });
    }

    @Override
    public Future<Integer> getFreeStorageNode(int remoteVerticleId, String zone, long objectSizeInBytes) {
        Buffer buffer = Buffer.buffer();
        buffer.appendByte((byte) RpcType.METADATA.ordinal())
                .appendByte((byte) MetadataRpcType.GET_FREE_STORAGE_NODE.ordinal())
                .appendInt(zone.length())
                .appendString(zone)
                .appendLong(objectSizeInBytes);
        return vertx.eventBus()
                .<Buffer>request(String.valueOf(remoteVerticleId), buffer)
                .transform(this::transformRequestResult)
                .map(replyBuffer -> {
                    return SerializationUtils.deserialize(replyBuffer.getBytes());
                });
    }

    private <T> Future<T> transformRequestResult(AsyncResult<Message<T>> res) {
        if (res.succeeded()) {
            // Return the body of the message
            return Future.succeededFuture(res.result().body());
        } else {
            logger.error("Metadata RPC failed", res.cause());

            // Transform the ReplyException to a MetadataRpcException
            ReplyException replyException = (ReplyException) res.cause();
            MetadataRpcErrorType errorType;
            switch (replyException.failureType()) {
                case RECIPIENT_FAILURE:
                    errorType = MetadataRpcErrorType.values()[replyException.failureCode()];
                    break;
                case TIMEOUT:
                    errorType = MetadataRpcErrorType.TIMEOUT;
                    break;
                case NO_HANDLERS:
                default:
                    errorType = MetadataRpcErrorType.UNKNOWN_ERROR;
                    break;
            }
            return Future.failedFuture(new MetadataRpcException(errorType, replyException.getMessage()));
        }
    }
}
