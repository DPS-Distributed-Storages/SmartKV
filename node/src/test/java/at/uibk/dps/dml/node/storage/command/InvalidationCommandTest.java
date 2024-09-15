package at.uibk.dps.dml.node.storage.command;

import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class InvalidationCommandTest {

    @Test
    void testEncodeDecode() {
        InvalidationCommand senderCmd = mock(InvalidationCommand.class,
                withSettings()
                        .useConstructor(null, "key", null, false, false, 17, false)
                        .defaultAnswer(Answers.CALLS_REAL_METHODS));
        Buffer buffer = Buffer.buffer();
        doNothing().when(senderCmd).encodePayload(buffer);
        senderCmd.encode(buffer);

        InvalidationCommand receiverCmd = mock(InvalidationCommand.class, Answers.CALLS_REAL_METHODS);
        receiverCmd.decode(new BufferReader(buffer));

        assertEquals("key", receiverCmd.getKey());
        assertEquals(17, receiverCmd.getOriginVerticleId());
    }
}
