package at.uibk.dps.dml.node.storage.rpc;

import at.uibk.dps.dml.node.billing.BillingService;
import at.uibk.dps.dml.node.billing.RPCRequestType;
import at.uibk.dps.dml.node.util.BufferReader;
import at.uibk.dps.dml.node.util.Timestamp;
import at.uibk.dps.dml.node.exception.*;
import at.uibk.dps.dml.node.storage.StorageService;
import at.uibk.dps.dml.node.storage.command.*;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles incoming storage RPC messages that are sent over the Vert.x event bus.
 */
public class StorageRpcHandler implements Handler<Message<Buffer>> {

    private final Logger logger = LoggerFactory.getLogger(StorageRpcHandler.class);

    private final StorageService storageService;

    private BillingService billingService;

    /**
     * Default constructor.
     *
     * @param storageService the storage service
     */
    public StorageRpcHandler(StorageService storageService, BillingService billingService) {
        this.storageService = storageService;
        this.billingService = billingService;
    }

    /**
     * Handles an incoming RPC message.
     *
     * @param message the message to handle
     */
    @Override
    public void handle(Message<Buffer> message) {
        int ingressMessageSizeInBytes = message.body().length();
        BufferReader bufferReader = new BufferReader(message.body());
        bufferReader.getByte(); // RPC type (not needed here)
        StorageRpcType commandType = StorageRpcType.values()[bufferReader.getByte()];
        final int originVerticleId = bufferReader.getInt();
        billingService.addRpcDataTransferIn(ingressMessageSizeInBytes, originVerticleId);
        switch (commandType) {
            case INVALIDATE:
                // We will not invalidate on Read Only commands! -> Always PUT
                billingService.addRpcRequest(RPCRequestType.INVALIDATE);
                handleInvalidationCommand(message, bufferReader, originVerticleId);
                break;
            case COMMIT:
                billingService.addRpcRequest(RPCRequestType.COMMIT);
                handleCommitCommand(message, bufferReader, originVerticleId);
                break;
            case GET_OBJECT:
                billingService.addRpcRequest(RPCRequestType.GET);
                handleGetObjectCommand(message, bufferReader, originVerticleId);
                break;
            case GET_FREE_MEMORY:
                billingService.addRpcRequest(RPCRequestType.GET);
                handleGetFreeMemoryCommand(message, bufferReader, originVerticleId);
                break;
            default:
                logger.error("Received an unknown RPC command type: {}", commandType);
                replyError(message, new UnknownCommandException("Unknown command type: " + commandType), originVerticleId);
        }
    }

    private void handleInvalidationCommand(Message<Buffer> message, BufferReader bufferReader, final int originVerticleId) {
        int originEpoch = bufferReader.getInt();
        Timestamp timestamp = readTimestamp(bufferReader);
        InvalidationCommandType commandType = InvalidationCommandType.values()[bufferReader.getByte()];

        InvalidationCommand command;
        switch (commandType) {
            case STATE_REPLICATION:
                command = new StateReplicationCommand();
                break;
            case LOCK:
                command = new LockCommand();
                break;
            case UNLOCK:
                command = new UnlockCommand();
                break;
            case INIT_OBJECT:
                command = new InitObjectCommand();
                break;
            case SET:
                command = new SetCommand();
                break;
            case INVOKE_METHOD:
                command = new InvokeMethodCommand();
                break;
            default:
                logger.error("Received an unknown invalidation command type: {}", commandType);
                replyError(message, new UnknownCommandException("Unknown invalidation command type: " + commandType), originVerticleId);
                return;
        }

        command.decode(bufferReader);

        storageService.invalidate(originVerticleId, originEpoch, timestamp, command)
                .onSuccess(reply_message -> {
                    message.reply(reply_message);
                    // On sucess, the reply body will be always null, so we don't need to monitor it as egress data transfer!
                })
                .onFailure(err -> replyError(message, err, originVerticleId));
    }

    private void handleCommitCommand(Message<Buffer> message, BufferReader bufferReader, final int originVerticleId) {
        int originEpoch = bufferReader.getInt();
        String key = readKey(bufferReader);
        Timestamp timestamp = readTimestamp(bufferReader);

        storageService.commit(originVerticleId, originEpoch, key, timestamp)
                .onSuccess(reply_message -> {
                    message.reply(reply_message);
                    // On sucess, the reply body will be always null, so we don't need to monitor it as egress data transfer!
                })
                .onFailure(err -> replyError(message, err, originVerticleId));
    }

    private void handleGetObjectCommand(Message<Buffer> message, BufferReader bufferReader, final int originVerticleId) {
        int originEpoch = bufferReader.getInt();
        String key = readKey(bufferReader);

        storageService.getObject(originEpoch, key)
                .map(storageObject -> Buffer.buffer(SerializationUtils.serialize(storageObject)))
                .onSuccess(reply_message -> {
                    message.reply(reply_message);
                    if(reply_message != null) {
                        billingService.addRpcDataTransferOut(reply_message.length(), originVerticleId);
                    }
                })
                .onFailure(err -> replyError(message, err, originVerticleId));
    }

    private void handleGetFreeMemoryCommand(Message<Buffer> message, BufferReader bufferReader, final int originVerticleId) {
        int originEpoch = bufferReader.getInt();
        storageService.getFreeMemory(originEpoch)
                .map(freeMemory -> Buffer.buffer(SerializationUtils.serialize(freeMemory)))
                .onSuccess(reply_message -> {
                    message.reply(reply_message);
                    if(reply_message != null) {
                        billingService.addRpcDataTransferOut(reply_message.length(), originVerticleId);
                    }
                })
                .onFailure(err -> replyError(message, err, originVerticleId));
    }

    private String readKey(BufferReader bufferReader) {
        int keyLength = bufferReader.getInt();
        return bufferReader.getString(keyLength);
    }

    private Timestamp readTimestamp(BufferReader bufferReader) {
        long timestampVersion = bufferReader.getLong();
        int timestampVerticleId = bufferReader.getInt();
        return new Timestamp(timestampVersion, timestampVerticleId);
    }

    private void replyError(Message<Buffer> message, Throwable err, final int originVerticleId) {
        int encodedException = encodeException(err);
        String errorMessage = err.getMessage();
        message.fail(encodedException, errorMessage);
        billingService.addTcpDataTransferOut(errorMessage.getBytes().length + Integer.BYTES, originVerticleId);
    }

    private int encodeException(Throwable err) {
        if (err instanceof UnknownCommandException) {
            return StorageRpcErrorType.UNKNOWN_COMMAND.ordinal();
        }
        if (err instanceof MissingMessagesException) {
            return StorageRpcErrorType.MISSING_MESSAGES.ordinal();
        }
        if (err instanceof ConflictingTimestampsException) {
            return StorageRpcErrorType.CONFLICTING_TIMESTAMPS.ordinal();
        }
        if (err instanceof CommandRejectedException) {
            return StorageRpcErrorType.REJECTED.ordinal();
        }
        if (err instanceof KeyDoesNotExistException) {
            return StorageRpcErrorType.KEY_DOES_NOT_EXIST.ordinal();
        }
        if (err instanceof ObjectNotReadyException) {
            return StorageRpcErrorType.OBJECT_NOT_READY.ordinal();
        }
        return StorageRpcErrorType.UNKNOWN_ERROR.ordinal();
    }
}
