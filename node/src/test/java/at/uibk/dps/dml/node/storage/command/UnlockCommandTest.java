package at.uibk.dps.dml.node.storage.command;

import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class UnlockCommandTest {

    @Test
    void testEncodePayload() {
        UnlockCommand cmd = new UnlockCommand();
        Buffer buffer = mock(Buffer.class);

        cmd.encodePayload(buffer);

        verifyNoInteractions(buffer);
    }

    @Test
    void testDecodePayload() {
        UnlockCommand cmd = new UnlockCommand();
        BufferReader buffer = mock(BufferReader.class);

        cmd.decodePayload(buffer);

        verifyNoInteractions(buffer);
    }
}
