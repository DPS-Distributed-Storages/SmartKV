package at.uibk.dps.dml.node.storage.command;

import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GetCommandTest {
    @Test
    void testEncodeDecodePayloadWithArgs() {
        GetCommand senderCmd = new GetCommand(null, "key", 0, 2, false);
        Buffer buffer = Buffer.buffer();
        senderCmd.encode(buffer);

        GetCommand receiverCmd = new GetCommand();
        receiverCmd.decode(new BufferReader(buffer));

        assertEquals("key", receiverCmd.getKey());
        assertEquals(InvalidationCommandType.GET, receiverCmd.getType());
        assertTrue(receiverCmd.isReadOnly());
        assertFalse(receiverCmd.isAllowInvalidReadsEnabled());
        assertFalse(receiverCmd.isAsyncReplicationEnabled());
    }
}
