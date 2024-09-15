package at.uibk.dps.dml.node;

import at.uibk.dps.dml.node.metadata.rpc.MetadataRpcHandler;
import at.uibk.dps.dml.node.storage.rpc.StorageRpcHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RpcRouterTest {

    @Mock
    Message<Buffer> message;

    @Test
    void testHandleMetadataMessage() {
        MetadataRpcHandler handler = mock(MetadataRpcHandler.class);
        RpcRouter rpcRouter = new RpcRouter(handler, null);
        Buffer buffer = Buffer.buffer()
                .appendByte((byte) RpcType.METADATA.ordinal());
        doNothing().when(handler).handle(message);
        when(message.body()).thenReturn(buffer);

        rpcRouter.handle(message);
    }

    @Test
    void testHandleStorageMessage() {
        StorageRpcHandler handler = mock(StorageRpcHandler.class);
        RpcRouter rpcRouter = new RpcRouter(null, handler);
        Buffer buffer = Buffer.buffer()
                .appendByte((byte) RpcType.STORAGE.ordinal());
        doNothing().when(handler).handle(message);
        when(message.body()).thenReturn(buffer);

        rpcRouter.handle(message);
    }
}
