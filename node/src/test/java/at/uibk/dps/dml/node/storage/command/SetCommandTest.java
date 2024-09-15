package at.uibk.dps.dml.node.storage.command;

import at.uibk.dps.dml.client.CommandType;
import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SetCommandTest {

    @Test
    void testEncodeDecodePayloadWithArgs() {
        SetCommand senderCmd = new SetCommand(null, "key", 2, 4, false, new byte[] {1,2,3});
        Buffer buffer = Buffer.buffer();
        senderCmd.encode(buffer);

        assertFalse(senderCmd.isReadOnly());
        assertFalse(senderCmd.isAllowInvalidReadsEnabled());
        assertFalse(senderCmd.isAsyncReplicationEnabled());

        SetCommand receiverCmd = new SetCommand();
        receiverCmd.decode(new BufferReader(buffer));

        assertEquals("key", receiverCmd.getKey());
        assertEquals(4, receiverCmd.getOriginVerticleId());
        assertEquals(InvalidationCommandType.SET, receiverCmd.getType());
        assertFalse(receiverCmd.isReadOnly());
        assertFalse(receiverCmd.isAllowInvalidReadsEnabled());
        assertFalse(receiverCmd.isAsyncReplicationEnabled());
        assertArrayEquals(new byte[] {1,2,3}, receiverCmd.getEncodedArgs());
    }

    @Test
    void testEncodeDecodePayloadWithoutArgs() {
        SetCommand senderCmd = new SetCommand(null, "key", 2, 4, false, null);
        Buffer buffer = Buffer.buffer();
        senderCmd.encode(buffer);

        SetCommand receiverCmd = new SetCommand();
        receiverCmd.decode(new BufferReader(buffer));

        assertEquals("key", receiverCmd.getKey());
        assertEquals(4, receiverCmd.getOriginVerticleId());
        assertEquals(InvalidationCommandType.SET, receiverCmd.getType());
        assertNull(receiverCmd.getEncodedArgs());
    }
}
