package at.uibk.dps.dml.node.storage.command;

import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InvokeMethodCommandTest {

    @Test
    void testEncodeDecodePayloadWithArgs() {
        InvokeMethodCommand senderCmd = new InvokeMethodCommand(null, "key", 0, false, false, 2, false, "method", new byte[] {1,2,3});
        Buffer buffer = Buffer.buffer();
        senderCmd.encode(buffer);

        InvokeMethodCommand receiverCmd = new InvokeMethodCommand();
        receiverCmd.decode(new BufferReader(buffer));

        assertEquals("method", receiverCmd.getMethodName());
        assertArrayEquals(new byte[] {1,2,3}, receiverCmd.getEncodedArgs());
    }

    @Test
    void testEncodeDecodePayloadWithoutArgs() {
        InvokeMethodCommand senderCmd = new InvokeMethodCommand(null, "key", 0, false, false, 2, false, "method", null);
        Buffer buffer = Buffer.buffer();
        senderCmd.encode(buffer);

        InvokeMethodCommand receiverCmd = new InvokeMethodCommand();
        receiverCmd.decode(new BufferReader(buffer));

        assertEquals("method", receiverCmd.getMethodName());
        assertNull(receiverCmd.getEncodedArgs());
    }
}
