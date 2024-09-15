package at.uibk.dps.dml.node.storage.command;

import at.uibk.dps.dml.node.storage.StorageObject;
import at.uibk.dps.dml.node.util.BufferReader;
import at.uibk.dps.dml.node.util.Timestamp;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StateReplicationCommandTest {

    @Test
    void testEncodeDecodePayloadWithArgs() {
        StorageObject storageObject = new StorageObject(new Timestamp(120, 2));
        StateReplicationCommand senderCmd = new StateReplicationCommand(null, "key", 0, 4, storageObject);
        Buffer buffer = Buffer.buffer();
        senderCmd.encode(buffer);

        StateReplicationCommand receiverCmd = new StateReplicationCommand();
        receiverCmd.decode(new BufferReader(buffer));

        assertEquals(storageObject.getTimestamp(), receiverCmd.getStorageObject().getTimestamp());
    }
}
