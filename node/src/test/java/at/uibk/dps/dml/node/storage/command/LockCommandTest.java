package at.uibk.dps.dml.node.storage.command;

import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class LockCommandTest {

    @Test
    void testEncodePayload() {
        LockCommand cmd = new LockCommand();
        Buffer buffer = mock(Buffer.class);

        cmd.encodePayload(buffer);

        verifyNoInteractions(buffer);
    }

    @Test
    void testDecodePayload() {
        LockCommand cmd = new LockCommand();
        BufferReader buffer = mock(BufferReader.class);

        cmd.decodePayload(buffer);

        verifyNoInteractions(buffer);
    }
}
