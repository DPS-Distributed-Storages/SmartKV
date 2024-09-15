package at.uibk.dps.dml.client.metadata.commands;

import at.uibk.dps.dml.client.Command;
import at.uibk.dps.dml.client.CommandType;
import at.uibk.dps.dml.client.metadata.KeyConfiguration;
import at.uibk.dps.dml.client.metadata.MetadataCommandType;
import at.uibk.dps.dml.client.metadata.Storage;
import at.uibk.dps.dml.client.util.ReadableBuffer;
import io.vertx.core.buffer.Buffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetAllCommand implements Command<Map<String, KeyConfiguration>> {

    @Override
    public CommandType getCommandType() {
        return MetadataCommandType.GETALL;
    }

    @Override
    public void encode(Buffer buffer) {
        // Nothing to encode
    }

    @Override
    public Map<String, KeyConfiguration> decodeReply(ReadableBuffer buffer) {
        Map<String, KeyConfiguration> result = new HashMap<>();
        int numKeys = buffer.getInt();
        for (int i = 0; i < numKeys; i++) {
            String key = buffer.getString(buffer.getInt());
            int metadataVersion = buffer.getInt();
            int numReplicas = buffer.getInt();
            List<Storage> replicas = new ArrayList<>(numReplicas);
            for (int j = 0; j < numReplicas; j++){
                replicas.add(new Storage(
                        buffer.getInt(),
                        buffer.getString(buffer.getInt()),
                        buffer.getString(buffer.getInt()),
                        buffer.getInt()
                ));
            }
            result.put(key, new KeyConfiguration(metadataVersion, replicas));
        }
        return result;
    }
}
