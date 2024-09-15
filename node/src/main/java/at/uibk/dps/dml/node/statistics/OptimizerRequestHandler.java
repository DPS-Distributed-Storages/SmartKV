package at.uibk.dps.dml.node.statistics;

import at.uibk.dps.dml.client.storage.BsonArgsCodec;
import at.uibk.dps.dml.client.util.BufferUtil;
import at.uibk.dps.dml.node.billing.TCPRequestType;
import at.uibk.dps.dml.node.exception.UnknownCommandException;
import at.uibk.dps.dml.node.membership.DmlNodeUnitPrices;
import at.uibk.dps.dml.node.statistics.command.OptimizerCommandErrorType;
import at.uibk.dps.dml.node.statistics.command.OptimizerCommandType;
import at.uibk.dps.dml.node.util.BufferReader;
import at.uibk.dps.dml.client.CommandResultType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;

import java.util.Map;
import java.util.Set;


/**
 * The {@link OptimizerRequestHandler} handles incoming requests from the optimizer
 */
public class OptimizerRequestHandler implements Handler<NetSocket> {

    private final StatisticManager statisticManager;

    private final String zone;

    public OptimizerRequestHandler(StatisticManager statisticManager, String zone) {
        this.statisticManager = statisticManager;
        this.zone = zone;
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
        OptimizerCommandType commandType = OptimizerCommandType.valueOf(bufferReader.getByte());

        switch (commandType) {
            case GET_STATISTICS:
                handleGetStatisticsCommand(socket, requestId);
                break;
            case GET_MAIN_OBJECT_LOCATIONS:
                handleGetMainObjectLocationsCommand(socket, requestId);
                break;
            case GET_UNIT_PRICES:
                handleGetPricesCommand(socket, requestId);
                break;
            case GET_BILLS:
                handleGetBillsCommand(socket, requestId);
                break;
            case CLEAR_STATISTICS:
                handleClearStatisticsCommand(socket, requestId);
                break;
            case GET_FREE_MEMORY:
                handleGetFreeMemoryCommand(socket, requestId);
                break;
            default:
                replyError(socket, requestId, new UnknownCommandException("Unknown command type: " + commandType));
        }
    }

    private void handleGetStatisticsCommand(NetSocket socket, int requestId) {

        statisticManager.provideStatistics()
                .onSuccess(records -> {
                    Buffer replyBuffer = initReplyBuffer(requestId, CommandResultType.SUCCESS);
                    BufferUtil.appendLengthPrefixedString(replyBuffer, this.zone);
                    replyBuffer.appendInt(records.length);
                    for(Record record : records) {
                        long timestamp = record.getTimestamp();
                        Map<StatisticKey, StatisticEntry> statistics = record.getStatistics();
                        replyBuffer.appendLong(timestamp);
                        replyBuffer.appendInt(statistics.size());
                        for (Map.Entry<StatisticKey, StatisticEntry> entry : statistics.entrySet()) {
                            BufferUtil.appendLengthPrefixedString(replyBuffer, entry.getKey().getKey());
                            BufferUtil.appendLengthPrefixedString(replyBuffer, entry.getKey().getClientZone());
                            encodeConfiguration(entry.getValue(), replyBuffer);
                        }
                        replyBuffer.setInt(0, replyBuffer.length() - 4);
                    }
                    socket.write(replyBuffer);
                })
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private void handleGetMainObjectLocationsCommand(NetSocket socket, int requestId) {

        statisticManager.getMainObjectLocations()
                .onSuccess(mainObjectLocations -> {
                    Buffer replyBuffer = initReplyBuffer(requestId, CommandResultType.SUCCESS);
                    replyBuffer.appendInt(mainObjectLocations.size());
                    for (Map.Entry<String, Set<Integer>> entry : mainObjectLocations.entrySet()){
                        BufferUtil.appendLengthPrefixedString(replyBuffer, entry.getKey());
                        encodeLocations(entry.getValue(), replyBuffer);
                    }
                    replyBuffer.setInt(0, replyBuffer.length() - 4);
                    socket.write(replyBuffer);
                })
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private void handleGetPricesCommand(NetSocket socket, int requestId) {

        statisticManager.getPrices()
                .onSuccess(unitPrices -> {
                    Buffer replyBuffer = initReplyBuffer(requestId, CommandResultType.SUCCESS);
                    encodeBytesObject(unitPrices, replyBuffer);
                    replyBuffer.setInt(0, replyBuffer.length() - 4);
                    socket.write(replyBuffer);
                })
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private void handleGetBillsCommand(NetSocket socket, int requestId) {

        statisticManager.getBills()
                .onSuccess(bills -> {
                    Buffer replyBuffer = initReplyBuffer(requestId, CommandResultType.SUCCESS);
                    encodeBytesObject(bills, replyBuffer);
                    replyBuffer.setInt(0, replyBuffer.length() - 4);
                    socket.write(replyBuffer);
                })
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private void handleClearStatisticsCommand(NetSocket socket, int requestId) {
        statisticManager.clearStatistics()
                .onSuccess(res -> {
                    Buffer replyBuffer = initReplyBuffer(requestId, CommandResultType.SUCCESS);
                    replyBuffer.setInt(0, replyBuffer.length() - 4);
                    socket.write(replyBuffer);
                })
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private void handleGetFreeMemoryCommand(NetSocket socket, int requestId) {

        statisticManager.getFreeMemory()
                .onSuccess(freeMemory -> {
                    Buffer replyBuffer = initReplyBuffer(requestId, CommandResultType.SUCCESS);
                    replyBuffer.appendLong(freeMemory);
                    replyBuffer.setInt(0, replyBuffer.length() - 4);
                    socket.write(replyBuffer);
                })
                .onFailure(err -> replyError(socket, requestId, err));
    }

    private void encodeConfiguration(StatisticEntry statisticEntry, Buffer replyBuffer) {
        replyBuffer.appendInt(statisticEntry.getNumberOfRequests().size());
        for (Map.Entry<TCPRequestType, Long> entry : statisticEntry.getNumberOfRequests().entrySet()) {
            BufferUtil.appendLengthPrefixedString(replyBuffer, entry.getKey().toString());
            replyBuffer.appendLong(entry.getValue());
        }

        replyBuffer.appendInt(statisticEntry.getCumulativeMessageSizeReceived().size());
        for (Map.Entry<TCPRequestType, Long> entry : statisticEntry.getCumulativeMessageSizeReceived().entrySet()) {
            BufferUtil.appendLengthPrefixedString(replyBuffer, entry.getKey().toString());
            replyBuffer.appendLong(entry.getValue());
        }

        replyBuffer.appendInt(statisticEntry.getCumulativeMessageSizeSent().size());
        for (Map.Entry<TCPRequestType, Long> entry : statisticEntry.getCumulativeMessageSizeSent().entrySet()) {
            BufferUtil.appendLengthPrefixedString(replyBuffer, entry.getKey().toString());
            replyBuffer.appendLong(entry.getValue());
        }
    }

    private void encodeBytesObject(byte[] bytes, Buffer replyBuffer) {
        if (bytes != null) {
            replyBuffer.appendInt(bytes.length).appendBytes(bytes);
        } else {
            replyBuffer.appendInt(-1);
        }
    }

    private void encodeLocations(Set<Integer> locations, Buffer replyBuffer) {
        replyBuffer.appendInt(locations.size());
        for (int verticleId : locations) {
            replyBuffer.appendInt(verticleId);
        }
    }

    private Buffer initReplyBuffer(int requestId, CommandResultType resultType) {
        return Buffer.buffer()
                .appendInt(0) // message length
                .appendInt(requestId)
                .appendByte(resultType.getId());
    }

    private int replyError(NetSocket socket, int requestId, Throwable throwable) {
        Buffer replyBuffer = initReplyBuffer(requestId, CommandResultType.ERROR);
        encodeException(replyBuffer, throwable);
        replyBuffer.setInt(0, replyBuffer.length() - 4);
        socket.write(replyBuffer);
        return replyBuffer.length();
    }

    private void encodeException(Buffer replyBuffer, Throwable err) {
        if (err instanceof UnknownCommandException) {
            replyBuffer.appendInt(OptimizerCommandErrorType.UNKNOWN_COMMAND.getErrorCode());
        } else {
            replyBuffer.appendInt(OptimizerCommandErrorType.UNKNOWN_ERROR.getErrorCode());
        }
        replyBuffer.appendInt(-1); // No error message
    }
}
