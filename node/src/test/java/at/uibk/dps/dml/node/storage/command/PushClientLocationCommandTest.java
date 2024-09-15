package at.uibk.dps.dml.node.storage.command;

import at.uibk.dps.dml.node.util.BufferReader;
import at.uibk.dps.dml.node.util.NodeLocation;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.net.impl.SocketAddressImpl;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

import static org.junit.jupiter.api.Assertions.*;

class PushClientLocationCommandTest {

    @Test
    void testPushClientLocationCommand() {
        SocketAddress socketAddress = new SocketAddressImpl(9000, "localhost");
        NodeLocation nodeLocation = new NodeLocation("local", "ap1", "local");
        PushClientLocationCommand senderCmd = new PushClientLocationCommand(null, socketAddress, nodeLocation);

        assertNull(senderCmd.getKey());
        assertEquals(socketAddress, senderCmd.getClientSocketAddress());
        assertEquals(nodeLocation, senderCmd.getClientLocation());
        assertEquals(nodeLocation.getProvider(), senderCmd.getClientProvider());
        assertEquals(nodeLocation.getRegion(), senderCmd.getClientRegion());
    }
}
