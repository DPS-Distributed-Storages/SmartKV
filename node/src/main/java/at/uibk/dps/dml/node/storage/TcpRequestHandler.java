package at.uibk.dps.dml.node.storage;

import at.uibk.dps.dml.client.storage.Flag;
import at.uibk.dps.dml.client.util.BufferUtil;
import at.uibk.dps.dml.node.metadata.MetadataEntry;
import at.uibk.dps.dml.node.metadata.MetadataService;
import at.uibk.dps.dml.node.billing.BillingService;
import at.uibk.dps.dml.node.billing.TCPRequestType;
import at.uibk.dps.dml.node.statistics.StatisticManager;
import at.uibk.dps.dml.node.util.BufferReader;
import at.uibk.dps.dml.client.CommandResultType;
import at.uibk.dps.dml.client.storage.StorageCommandErrorType;
import at.uibk.dps.dml.client.storage.StorageCommandType;
import at.uibk.dps.dml.node.exception.*;
import at.uibk.dps.dml.node.util.NodeLocation;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;

import java.util.EnumSet;

/**
 * The {@link TcpRequestHandler} handles incoming TCP requests from clients
 */
public class TcpRequestHandler implements Handler<NetSocket> {

    private final StorageService storageService;
    private final MetadataService metadataService;

    private final BillingService billingService;

    private final StatisticManager statisticManager;

    public TcpRequestHandler(StorageService storageService, MetadataService metadataService, BillingService billingService, StatisticManager statisticManager) {
        this.storageService = storageService;
        this.metadataService = metadataService;
        this.billingService = billingService;
        this.statisticManager = statisticManager;
    }

    @Override
    public void handle(NetSocket socket) {
        final RecordParser parser = RecordParser.newFixed(4);
        Handler<Buffer> handler = new Handler<>() {
            boolean readMessageLength = true;

            @Override
            public void handle(Buffer buffer) {
                if (readMessageLength) {
                    readMessageLength = false;
                    parser.fixedSizeMode(buffer.getInt(0));
                } else {
                    readMessageLength = true;
                    parser.fixedSizeMode(4);
                    handleRequest(socket, buffer);
                }
            }
        };
        parser.handler(handler);
        socket.handler(parser);
    }

    private void handleRequest(NetSocket socket, Buffer commandBuffer) {
        int ingressMessageSizeInBytes = commandBuffer.length();
        BufferReader bufferReader = new BufferReader(commandBuffer);
        int requestId = bufferReader.getInt();
        StorageCommandType commandType = StorageCommandType.valueOf(bufferReader.getByte());

        // We will track ingress dataTransfer for all commands, except for PUSH_CLIENT_LOCATION commands
        if(commandType != StorageCommandType.PUSH_CLIENT_LOCATION){
            NodeLocation clientLocation = storageService.getClientLocation(socket.remoteAddress());
            billingService.addTcpDataTransferIn(ingressMessageSizeInBytes, clientLocation);
        }

        switch (commandType) {
            case LOCK:
                billingService.addTcpRequest(TCPRequestType.PUT);
                handleLockCommand(socket, requestId, bufferReader);
                break;
            case UNLOCK:
                billingService.addTcpRequest(TCPRequestType.PUT);
                handleUnlockCommand(socket, requestId, bufferReader);
                break;
            case INIT_OBJECT:
                billingService.addTcpRequest(TCPRequestType.PUT);
                handleInitObjectCommand(socket, requestId, bufferReader);
                break;
            case GET:
                billingService.addTcpRequest(TCPRequestType.GET);
                handleGetCommand(socket, requestId, bufferReader, ingressMessageSizeInBytes);
                break;
            case SET:
                billingService.addTcpRequest(TCPRequestType.PUT);
                handleSetCommand(socket, requestId, bufferReader, ingressMessageSizeInBytes);
                break;
            case INVOKE_METHOD:
                handleInvokeMethodCommand(socket, requestId, bufferReader, ingressMessageSizeInBytes);
                break;
            case PUSH_CLIENT_LOCATION:
                handlePushClientLocationCommand(socket, requestId, bufferReader);
                break;
            default:
                replyError(socket, requestId, new UnknownCommandException("Unknown command type: " + commandType));
        }
    }

    private void handleLockCommand(NetSocket socket, int requestId, BufferReader bufferReader) {
        String key = bufferReader.getLengthPrefixedString();

        storageService.lock(key)
                .onSuccess(lockToken -> {
                    Buffer replyBuffer = initReplyBufferWithSuccessResultAndMetadataVersion(requestId, key);
                    replyBuffer.appendInt(lockToken);
                    replyBuffer.setInt(0, replyBuffer.length() - 4);
                    NodeLocation clientLocation = storageService.getClientLocation(socket.remoteAddress());
                    billingService.addTcpDataTransferOut(replyBuffer.length(), clientLocation);
                    socket.write(replyBuffer);
                })
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private void handleUnlockCommand(NetSocket socket, int requestId, BufferReader bufferReader) {
        String key = bufferReader.getLengthPrefixedString();
        int lockToken = bufferReader.getInt();

        storageService.unlock(key, lockToken)
                .onSuccess(res -> replySuccess(socket, requestId, key))
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private void handleInitObjectCommand(NetSocket socket, int requestId, BufferReader bufferReader) {
        String key = bufferReader.getLengthPrefixedString();
        String languageId = bufferReader.getLengthPrefixedString();
        String objectType = bufferReader.getLengthPrefixedString();
        int argsLength = bufferReader.getInt();
        byte[] encodedArgs = argsLength >= 0 ? bufferReader.getBytes(argsLength) : null;

        Integer lockToken = null;
        if (!bufferReader.reachedEnd()) {
            lockToken = bufferReader.getInt();
        }

        storageService.initObject(key, languageId, objectType, encodedArgs, lockToken)
                .onSuccess(res -> replySuccess(socket, requestId, key))
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private void handleInvokeMethodCommand(NetSocket socket, int requestId, BufferReader bufferReader, int ingressMessageSizeInBytes) {
        String key = bufferReader.getLengthPrefixedString();
        NodeLocation clientLocation = storageService.getClientLocation(socket.remoteAddress());

        String methodName = bufferReader.getLengthPrefixedString();
        int argsLength = bufferReader.getInt();
        byte[] encodedArgs = argsLength >= 0 ? bufferReader.getBytes(argsLength) : null;

        byte flagsBitVector = bufferReader.getByte();
        EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
        for (Flag flag : Flag.values()) {
            if ((flagsBitVector & flag.getValue()) != 0) {
                flags.add(flag);
            }
        }

        Integer lockToken = null;
        if (!bufferReader.reachedEnd()) {
            lockToken = bufferReader.getInt();
        }

        boolean isReadOnly = flags.contains(Flag.READ_ONLY);
        TCPRequestType requestType = isReadOnly ? TCPRequestType.GET : TCPRequestType.PUT;

        billingService.addTcpRequest(requestType);

        storageService.invokeMethod(key, methodName, encodedArgs, lockToken, flags)
                .onSuccess(result -> {
                    Buffer replyBuffer = initReplyBufferWithSuccessResultAndMetadataVersion(requestId, key);
                    if (result != null) {
                        replyBuffer.appendInt(result.length).appendBytes(result);
                    } else {
                        replyBuffer.appendInt(-1);
                    }
                    replyBuffer.setInt(0, replyBuffer.length() - 4);
                    billingService.addTcpDataTransferOut(replyBuffer.length(), clientLocation);
                    statisticManager.addStatistic(requestType, ingressMessageSizeInBytes, replyBuffer.length(), key, clientLocation.getZone());
                    socket.write(replyBuffer);
                })
                .onFailure(err -> replyErrorAndWriteStatistic(socket, requestId, err, key, requestType, ingressMessageSizeInBytes));
    }

    private void handleGetCommand(NetSocket socket, int requestId, BufferReader bufferReader, int ingressMessageSizeInBytes) {
        NodeLocation clientLocation = storageService.getClientLocation(socket.remoteAddress());
        String key = bufferReader.getLengthPrefixedString();

        byte flagsBitVector = bufferReader.getByte();
        EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
        for (Flag flag : Flag.values()) {
            if ((flagsBitVector & flag.getValue()) != 0) {
                flags.add(flag);
            }
        }

        Integer lockToken = null;
        if (!bufferReader.reachedEnd()) {
            lockToken = bufferReader.getInt();
        }

        storageService.get(key, lockToken, flags)
                .onSuccess(result -> {
                    Buffer replyBuffer = initReplyBufferWithSuccessResultAndMetadataVersion(requestId, key);
                    if (result != null) {
                        replyBuffer.appendInt(result.length).appendBytes(result);
                    } else {
                        replyBuffer.appendInt(-1);
                    }
                    replyBuffer.setInt(0, replyBuffer.length() - 4);

                    billingService.addTcpDataTransferOut(replyBuffer.length(), clientLocation);
                    statisticManager.addStatistic(TCPRequestType.GET, ingressMessageSizeInBytes, replyBuffer.length(), key, clientLocation.getZone());
                    socket.write(replyBuffer);
                })
                .onFailure(err -> replyErrorAndWriteStatistic(socket, requestId, err, key, TCPRequestType.GET, ingressMessageSizeInBytes));
    }

    private void handleSetCommand(NetSocket socket, int requestId, BufferReader bufferReader, int ingressMessageSizeInBytes) {
        String key = bufferReader.getLengthPrefixedString();
        NodeLocation clientLocation = storageService.getClientLocation(socket.remoteAddress());

        int argsLength = bufferReader.getInt();
        byte[] encodedArgs = argsLength >= 0 ? bufferReader.getBytes(argsLength) : null;

        byte flagsBitVector = bufferReader.getByte();
        EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
        for (Flag flag : Flag.values()) {
            if ((flagsBitVector & flag.getValue()) != 0) {
                flags.add(flag);
            }
        }

        Integer lockToken = null;
        if (!bufferReader.reachedEnd()) {
            lockToken = bufferReader.getInt();
        }

        storageService.set(key, encodedArgs, lockToken, flags)
                .onSuccess(res -> replySuccessAndWriteStatistic(socket, requestId, key, TCPRequestType.PUT, ingressMessageSizeInBytes))
                .onFailure(err -> replyErrorAndWriteStatistic(socket, requestId, err, key, TCPRequestType.PUT, ingressMessageSizeInBytes));
    }

    private void handlePushClientLocationCommand(NetSocket socket, int requestId, BufferReader bufferReader) {
        String region = bufferReader.getLengthPrefixedString();
        String zone = bufferReader.getLengthPrefixedString();
        String provider = bufferReader.getLengthPrefixedString();

        storageService.registerClientLocation(socket.remoteAddress(), new NodeLocation(region, zone, provider))
                .onSuccess(res -> {
                    Buffer replyBuffer = initReplyBuffer(requestId, CommandResultType.SUCCESS);
                    replyBuffer.setInt(0, replyBuffer.length() - 4);
                    socket.write(replyBuffer);
                })
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private int getMetadataVersion(String key) {
        MetadataEntry entry = metadataService.getMetadataEntry(key);
        if (entry == null) {
            return -1;
        }
        return (int) entry.getTimestamp().getVersion();
    }

    private Buffer initReplyBuffer(int requestId, CommandResultType resultType) {
        return Buffer.buffer()
                .appendInt(0) // message length
                .appendInt(requestId)
                .appendByte(resultType.getId());
    }

    private Buffer initReplyBufferWithSuccessResultAndMetadataVersion(int requestId, String key) {
        return initReplyBuffer(requestId, CommandResultType.SUCCESS)
                .appendInt(getMetadataVersion(key));
    }

    private int replySuccess(NetSocket socket, int requestId, String key) {
        Buffer replyBuffer = initReplyBufferWithSuccessResultAndMetadataVersion(requestId, key);
        replyBuffer.setInt(0, replyBuffer.length() - 4);
        NodeLocation clientLocation = storageService.getClientLocation(socket.remoteAddress());
        billingService.addTcpDataTransferOut(replyBuffer.length(), clientLocation);
        socket.write(replyBuffer);
        return replyBuffer.length();
    }

    private void replySuccessAndWriteStatistic(NetSocket socket, int requestId, String key, TCPRequestType requestType, int ingressMessageSizeInBytes){
        int successMessageSize = replySuccess(socket, requestId, key);
        NodeLocation clientLocation = storageService.getClientLocation(socket.remoteAddress());
        statisticManager.addStatistic(requestType, ingressMessageSizeInBytes, successMessageSize, key, clientLocation.getZone());
    }

    private int replyError(NetSocket socket, int requestId, Throwable throwable) {
        Buffer replyBuffer = initReplyBuffer(requestId, CommandResultType.ERROR);
        encodeException(replyBuffer, throwable);
        replyBuffer.setInt(0, replyBuffer.length() - 4);
        NodeLocation clientLocation = storageService.getClientLocation(socket.remoteAddress());
        billingService.addTcpDataTransferOut(replyBuffer.length(), clientLocation);
        socket.write(replyBuffer);
        return replyBuffer.length();
    }

    private void replyErrorAndWriteStatistic(NetSocket socket, int requestId, Throwable throwable, String key, TCPRequestType requestType, int ingressMessageSizeInBytes){
        int errorMessageSize = replyError(socket, requestId, throwable);
        NodeLocation clientLocation = storageService.getClientLocation(socket.remoteAddress());
        statisticManager.addStatistic(requestType, ingressMessageSizeInBytes, errorMessageSize, key, clientLocation.getZone());
    }

    private void encodeException(Buffer replyBuffer, Throwable err) {
        if (err instanceof SharedObjectException) {
            replyBuffer.appendInt(StorageCommandErrorType.SHARED_OBJECT_ERROR.getErrorCode());
            String message = err.getCause().getMessage();
            if (message == null) {
                message = err.getCause().getClass().getName();
            }
            BufferUtil.appendLengthPrefixedString(replyBuffer, message);
            return;
        }
        if (err instanceof UnknownCommandException) {
            replyBuffer.appendInt(StorageCommandErrorType.UNKNOWN_COMMAND.getErrorCode());
        } else if (err instanceof KeyDoesNotExistException) {
            replyBuffer.appendInt(StorageCommandErrorType.KEY_DOES_NOT_EXIST.getErrorCode());
        } else if (err instanceof InvalidLockTokenException) {
            replyBuffer.appendInt(StorageCommandErrorType.INVALID_LOCK_TOKEN.getErrorCode());
        } else if (err instanceof NotResponsibleException) {
            replyBuffer.appendInt(StorageCommandErrorType.NOT_RESPONSIBLE.getErrorCode());
        } else if (err instanceof ObjectNotInitializedException) {
            replyBuffer.appendInt(StorageCommandErrorType.OBJECT_NOT_INITIALIZED.getErrorCode());
        } else {
            replyBuffer.appendInt(StorageCommandErrorType.UNKNOWN_ERROR.getErrorCode());
        }
        replyBuffer.appendInt(-1); // No error message
    }
}
