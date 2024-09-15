package at.uibk.dps.dml.client;

import at.uibk.dps.dml.client.util.ReadableBuffer;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseTcpClient {

    private final Logger logger = LoggerFactory.getLogger(BaseTcpClient.class);

    private final Vertx vertx;

    private NetClient netClient;

    /**
     * Maps request IDs to reply handlers.
     */
    private final Map<Integer, Handler<ReadableBuffer>> replyHandlerMap = new HashMap<>();

    private NetSocket netSocket;

    private int requestIdCounter = 0;

    protected BaseTcpClient(Vertx vertx) {
        this.vertx = vertx;
    }

    public Future<Void> connect(String host, int port) {
        Promise<Void> promise = Promise.promise();
        this.netClient = vertx.createNetClient();
        netClient.connect(port, host)
                .onSuccess(socket -> {
                    netSocket = socket;
                    final RecordParser parser = RecordParser.newFixed(4);
                    parser.handler(new Handler<>() {
                        boolean readMessageLength = true;

                        @Override
                        public void handle(Buffer buffer) {
                            if (readMessageLength) {
                                readMessageLength = false;
                                int messageLength = buffer.getInt(0);
                                parser.fixedSizeMode(messageLength);
                            } else {
                                readMessageLength = true;
                                parser.fixedSizeMode(4);
                                ReadableBuffer readableBuffer = new ReadableBuffer(buffer);
                                int requestId = readableBuffer.getInt();
                                Handler<ReadableBuffer> handler = replyHandlerMap.remove(requestId);
                                if (handler != null) {
                                    handler.handle(readableBuffer);
                                } else {
                                    logger.error("Unknown request ID");
                                }
                            }
                        }
                    });
                    netSocket.handler(parser);
                    promise.complete();
                })
                .onFailure(promise::fail);
        return promise.future();
    }

    public Future<Void> disconnect() {
        return netClient.close();
    }

    public BaseTcpClient disconnectHandler(Handler<Void> handler) {
        netSocket.closeHandler(handler);
        return this;
    }

    protected <R> Future<R> request(Command<R> command) {
        Buffer sendBuffer = initSendBuffer(command.getCommandType());
        command.encode(sendBuffer);
        int requestId = generateRequestId();
        updateSendBufferHeader(sendBuffer, requestId);

        netSocket.write(sendBuffer);

        Promise<R> promise = Promise.promise();
        Handler<ReadableBuffer> replyHandler = replyBuffer -> {
            switch (CommandResultType.valueOf(replyBuffer.getByte())) {
                case SUCCESS:
                    R reply = command.decodeReply(replyBuffer);
                    promise.complete(reply);
                    break;
                case ERROR:
                    int errorCode = replyBuffer.getInt();
                    String errorMessage = replyBuffer.getLengthPrefixedString();
                    promise.fail(decodeCommandResultError(errorCode, errorMessage));
                    break;
                default:
                    logger.error("Received unknown result type");
                    promise.fail(new IllegalStateException());
            }
        };

        replyHandlerMap.put(requestId, replyHandler);
        return promise.future();
    }

    protected abstract Throwable decodeCommandResultError(int errorCode, String message);

    private int generateRequestId() {
        return requestIdCounter++;
    }

    private Buffer initSendBuffer(CommandType commandType) {
        return Buffer.buffer()
                .appendInt(0) // message length (we set the actual value before sending the message)
                .appendInt(0) // request ID (we set the actual value before sending the message)
                .appendByte(commandType.getId());
    }

    private void updateSendBufferHeader(Buffer buffer, int requestId) {
        buffer.setInt(0, buffer.length() - 4); // message length
        buffer.setInt(4, requestId); // request ID
    }
}
