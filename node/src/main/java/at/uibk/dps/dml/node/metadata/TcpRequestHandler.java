package at.uibk.dps.dml.node.metadata;

import at.uibk.dps.dml.client.metadata.KeyConfiguration;
import at.uibk.dps.dml.client.util.BufferUtil;
import at.uibk.dps.dml.node.membership.MembershipView;
import at.uibk.dps.dml.node.util.BufferReader;
import at.uibk.dps.dml.client.CommandResultType;
import at.uibk.dps.dml.client.metadata.MetadataCommandErrorType;
import at.uibk.dps.dml.client.metadata.MetadataCommandType;
import at.uibk.dps.dml.client.metadata.Storage;
import at.uibk.dps.dml.node.exception.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;

import java.util.*;

public class TcpRequestHandler implements Handler<NetSocket> {

    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private final MetadataService metadataService;

    public TcpRequestHandler(MetadataService metadataService) {
        this.metadataService = metadataService;
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
        BufferReader bufferReader = new BufferReader(commandBuffer);
        int requestId = bufferReader.getInt();
        MetadataCommandType commandType = MetadataCommandType.valueOf(bufferReader.getByte());
        switch (commandType) {
            case CREATE:
                handleCreateCommand(socket, requestId, bufferReader);
                break;
            case GET:
                handleGetCommand(socket, requestId, bufferReader);
                break;
            case GETALL:
                handleGetAllCommand(socket, requestId);
                break;
            case DELETE:
                handleDeleteCommand(socket, requestId, bufferReader);
                break;
            case RECONFIGURE:
                handleReconfigureCommand(socket, requestId, bufferReader);
                break;
            case SYNCHRONIZED_RECONFIGURE:
                handleSynchronizedReconfigureCommand(socket, requestId, bufferReader);
                break;
            case GET_MEMBERSHIP_VIEW:
                handleGetMembershipViewCommand(socket, requestId);
                break;
            case GET_ZONE_INFO:
                handleGetZoneInfoCommand(socket, requestId, bufferReader);
                break;
            case GET_FREE_STORAGE_NODES:
                handleGetFreeStorageNodesCommand(socket, requestId, bufferReader);
                break;
            default:
                replyError(socket, requestId, new UnknownCommandException("Unknown command type: " + commandType));
        }
    }

    private void handleCreateCommand(NetSocket socket, int requestId, BufferReader bufferReader) {
        String key = readKey(bufferReader);
        boolean fullReplication = bufferReader.getInt() == 1 ? true : false;
        int numberOfReplicas = bufferReader.getInt();

        Set<Integer> replicaVerticleIds = new LinkedHashSet<>();
        for (int i = 0; i < numberOfReplicas; i++) {
            replicaVerticleIds.add(bufferReader.getInt());
        }

        metadataService.create(key, replicaVerticleIds, fullReplication)
                .onSuccess(res -> replySuccess(socket, requestId))
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private void handleGetCommand(NetSocket socket, int requestId, BufferReader bufferReader) {
        String key = readKey(bufferReader);

        metadataService.get(key)
                .onSuccess(configuration -> {
                    Buffer replyBuffer = initReplyBuffer(requestId, CommandResultType.SUCCESS);
                    encodeConfiguration(configuration, replyBuffer);
                    replyBuffer.setInt(0, replyBuffer.length() - 4);
                    socket.write(replyBuffer);
                })
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private void handleGetAllCommand(NetSocket socket, int requestId) {
        metadataService.getAll()
                .onSuccess(configurations -> {
                    Buffer replyBuffer = initReplyBuffer(requestId, CommandResultType.SUCCESS);
                    replyBuffer.appendInt(configurations.size());
                    for (Map.Entry<String, KeyConfiguration> entry : configurations.entrySet()){
                        BufferUtil.appendLengthPrefixedString(replyBuffer, entry.getKey());
                        encodeConfiguration(entry.getValue(), replyBuffer);
                    }
                    replyBuffer.setInt(0, replyBuffer.length() - 4);
                    socket.write(replyBuffer);
                })
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private void handleDeleteCommand(NetSocket socket, int requestId, BufferReader bufferReader) {
        String key = readKey(bufferReader);

        metadataService.delete(key)
                .onSuccess(res -> replySuccess(socket, requestId))
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private void handleReconfigureCommand(NetSocket socket, int requestId, BufferReader bufferReader) {
        String key = readKey(bufferReader);
        int numberOfNewReplicas = bufferReader.getInt();

        Set<Integer> newReplicaVerticleIds = new LinkedHashSet<>();
        for (int i = 0; i < numberOfNewReplicas; i++) {
            newReplicaVerticleIds.add(bufferReader.getInt());
        }

        metadataService.reconfigure(key, newReplicaVerticleIds)
                .onSuccess(res -> replySuccess(socket, requestId))
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private void handleSynchronizedReconfigureCommand(NetSocket socket, int requestId, BufferReader bufferReader) {
        String key = readKey(bufferReader);

        int numberOfOldReplicas = bufferReader.getInt();
        Set<Integer> oldReplicaVerticleIds = new LinkedHashSet<>();
        for (int i = 0; i < numberOfOldReplicas; i++) {
            oldReplicaVerticleIds.add(bufferReader.getInt());
        }

        int numberOfNewReplicas = bufferReader.getInt();
        Set<Integer> newReplicaVerticleIds = new LinkedHashSet<>();
        for (int i = 0; i < numberOfNewReplicas; i++) {
            newReplicaVerticleIds.add(bufferReader.getInt());
        }

        metadataService.synchronizedReconfigure(key, oldReplicaVerticleIds, newReplicaVerticleIds)
                .onSuccess(res -> replySuccess(socket, requestId))
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private void handleGetMembershipViewCommand(NetSocket socket, int requestId) {
        try {
            MembershipView membershipView = metadataService.getMembershipView();
            String membershipViewJson = jsonMapper.writeValueAsString(membershipView);
            Buffer replyBuffer = initReplyBuffer(requestId, CommandResultType.SUCCESS);
            BufferUtil.appendLengthPrefixedString(replyBuffer, membershipViewJson);
            replyBuffer.setInt(0, replyBuffer.length() - 4);
            socket.write(replyBuffer);
        } catch (JsonProcessingException e) {
            replyError(socket, requestId, e);
        }
    }

    private void handleGetZoneInfoCommand(NetSocket socket, int requestId, BufferReader bufferReader) {
        String zone = readKey(bufferReader);
        metadataService.getZoneInfo(zone)
                .onSuccess(zoneInfo -> {
                    try {
                        Buffer replyBuffer = initReplyBuffer(requestId, CommandResultType.SUCCESS);
                        String zoneInfoJson = jsonMapper.writeValueAsString(zoneInfo);
                        BufferUtil.appendLengthPrefixedString(replyBuffer, zoneInfoJson);
                        replyBuffer.setInt(0, replyBuffer.length() - 4);
                        socket.write(replyBuffer);
                    } catch (JsonProcessingException e) {
                        replyError(socket, requestId, e);
                    }
                })
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private void handleGetFreeStorageNodesCommand(NetSocket socket, int requestId, BufferReader bufferReader) {
        long objectSizeInBytes = bufferReader.getLong();
        int numberOfZones = bufferReader.getInt();

        Set<String> zones = new LinkedHashSet<>();
        for (int i = 0; i < numberOfZones; i++) {
            zones.add(bufferReader.getLengthPrefixedString());
        }

        metadataService.selectFreeStoragesFromZones(zones, objectSizeInBytes)
                .onSuccess(verticleIds -> {
                    try {
                        Buffer replyBuffer = initReplyBuffer(requestId, CommandResultType.SUCCESS);
                        String verticleIdJson = jsonMapper.writeValueAsString(verticleIds);
                        BufferUtil.appendLengthPrefixedString(replyBuffer, verticleIdJson);
                        replyBuffer.setInt(0, replyBuffer.length() - 4);
                        socket.write(replyBuffer);
                    } catch (JsonProcessingException e) {
                        replyError(socket, requestId, e);
                    }
                })
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private String readKey(BufferReader bufferReader) {
        int keyLength = bufferReader.getInt();
        return bufferReader.getString(keyLength);
    }

    private Buffer initReplyBuffer(int requestId, CommandResultType resultType) {
        return Buffer.buffer()
                .appendInt(0) // message length
                .appendInt(requestId)
                .appendByte(resultType.getId());
    }

    private void replySuccess(NetSocket socket, int requestId) {
        Buffer replyBuffer = initReplyBuffer(requestId, CommandResultType.SUCCESS);
        replyBuffer.setInt(0, replyBuffer.length() - 4);
        socket.write(replyBuffer);
    }

    private void replyError(NetSocket socket, int requestId, Throwable throwable) {
        Buffer replyBuffer = initReplyBuffer(requestId, CommandResultType.ERROR);
        int errorCode = encodeException(throwable);
        replyBuffer.appendInt(errorCode);
        replyBuffer.appendInt(-1); // No error message
        replyBuffer.setInt(0, replyBuffer.length() - 4);
        socket.write(replyBuffer);
    }

    private int encodeException(Throwable err) {
        if (err instanceof UnknownCommandException) {
            return MetadataCommandErrorType.UNKNOWN_COMMAND.getErrorCode();
        }
        if (err instanceof KeyDoesNotExistException) {
            return MetadataCommandErrorType.KEY_DOES_NOT_EXIST.getErrorCode();
        }
        if (err instanceof KeyAlreadyExistsException) {
            return MetadataCommandErrorType.KEY_ALREADY_EXISTS.getErrorCode();
        }
        if (err instanceof ConcurrentOperationException) {
            return MetadataCommandErrorType.CONCURRENT_OPERATION.getErrorCode();
        }
        if (err instanceof InvalidObjectLocationsException) {
            return MetadataCommandErrorType.INVALID_OBJECT_LOCATIONS.getErrorCode();
        }
        return MetadataCommandErrorType.UNKNOWN_ERROR.getErrorCode();
    }

    private void encodeConfiguration(KeyConfiguration configuration, Buffer replyBuffer) {
        replyBuffer.appendInt(configuration.getVersion());
        replyBuffer.appendInt(configuration.getReplicas().size());
        for (Storage replica : configuration.getReplicas()) {
            replyBuffer.appendInt(replica.getId());
            BufferUtil.appendLengthPrefixedString(replyBuffer, replica.getRegion());
            BufferUtil.appendLengthPrefixedString(replyBuffer, replica.getHostname());
            replyBuffer.appendInt(replica.getPort());
        }
    }
}
