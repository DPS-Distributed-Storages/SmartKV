package at.uibk.dps.dml.client.metadata.commands;

import at.uibk.dps.dml.client.Command;
import at.uibk.dps.dml.client.CommandType;
import at.uibk.dps.dml.client.metadata.KeyConfiguration;
import at.uibk.dps.dml.client.metadata.MetadataCommandType;
import at.uibk.dps.dml.client.metadata.Storage;
import at.uibk.dps.dml.client.util.ReadableBuffer;
import io.vertx.core.buffer.Buffer;

import java.util.ArrayList;
import java.util.List;

public class GetCommand implements Command<KeyConfiguration> {

    private final String key;

    public GetCommand(String key) {
        this.key = key;
    }

    @Override
    public CommandType getCommandType() {
        return MetadataCommandType.GET;
    }

    @Override
    public void encode(Buffer buffer) {
        buffer.appendInt(key.length()).appendString(key);
    }

    @Override
    public KeyConfiguration decodeReply(ReadableBuffer buffer) {
        int metadataVersion = buffer.getInt();
        int numReplicas = buffer.getInt();
        List<Storage> replicas = new ArrayList<>(numReplicas);
        for (int i = 0; i < numReplicas; i++) {
            replicas.add(new Storage(
                    buffer.getInt(),
                    buffer.getString(buffer.getInt()),
                    buffer.getString(buffer.getInt()),
                    buffer.getInt()
            ));
        }
        return new KeyConfiguration(metadataVersion, replicas);
    }
}
