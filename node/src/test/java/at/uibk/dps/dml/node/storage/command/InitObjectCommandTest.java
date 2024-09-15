package at.uibk.dps.dml.node.storage.command;

import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InitObjectCommandTest {

    @Test
    void testEncodeDecodePayloadWithArgs() {
        InitObjectCommand senderCmd = new InitObjectCommand(null, "key", null, 2, "lang", "type", new byte[] {1,2,3});
        Buffer buffer = Buffer.buffer();
        senderCmd.encode(buffer);

        InitObjectCommand receiverCmd = new InitObjectCommand();
        receiverCmd.decode(new BufferReader(buffer));

        assertEquals("lang", receiverCmd.getLanguageId());
        assertEquals("type", receiverCmd.getObjectType());
        assertArrayEquals(new byte[] {1,2,3}, receiverCmd.getEncodedArgs());
    }

    @Test
    void testEncodeDecodePayloadWithoutArgs() {
        InitObjectCommand senderCmd = new InitObjectCommand(null, "key", null, 2, "lang", "type", null);
        Buffer buffer = Buffer.buffer();
        senderCmd.encode(buffer);

        InitObjectCommand receiverCmd = new InitObjectCommand();
        receiverCmd.decode(new BufferReader(buffer));

        assertEquals("lang", receiverCmd.getLanguageId());
        assertEquals("type", receiverCmd.getObjectType());
        assertNull(receiverCmd.getEncodedArgs());
    }
}
