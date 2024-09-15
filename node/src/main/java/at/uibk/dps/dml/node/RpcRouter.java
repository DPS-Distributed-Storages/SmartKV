package at.uibk.dps.dml.node;

import at.uibk.dps.dml.node.metadata.rpc.MetadataRpcHandler;
import at.uibk.dps.dml.node.storage.rpc.StorageRpcHandler;
import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;

/**
 * Forwards incoming RPC requests to the corresponding handler.
 */
public class RpcRouter implements Handler<Message<Buffer>> {

    private final MetadataRpcHandler metadataRpcHandler;

    private final StorageRpcHandler storageRpcHandler;

    /**
     * Default constructor.
     *
     * @param metadataRpcHandler the handler for metadata RPC requests
     * @param storageRpcHandler the handler for storage RPC requests
     */
    public RpcRouter(MetadataRpcHandler metadataRpcHandler, StorageRpcHandler storageRpcHandler) {
        this.metadataRpcHandler = metadataRpcHandler;
        this.storageRpcHandler = storageRpcHandler;
    }

    /**
     * Checks the type of the incoming RPC request and forwards it to the corresponding handler.
     *
     * @param message the message to handle
     */
    @Override
    public void handle(Message<Buffer> message) {
        BufferReader bufferReader = new BufferReader(message.body());
        RpcType rpcType = RpcType.values()[bufferReader.getByte()];
        switch (rpcType) {
            case METADATA:
                metadataRpcHandler.handle(message);
                break;
            case STORAGE:
                storageRpcHandler.handle(message);
                break;
            default:
                throw new IllegalStateException("Invalid command");
        }
    }
}
