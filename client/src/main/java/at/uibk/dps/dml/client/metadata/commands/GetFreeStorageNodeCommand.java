package at.uibk.dps.dml.client.metadata.commands;

import at.uibk.dps.dml.client.*;
import at.uibk.dps.dml.client.metadata.*;
import at.uibk.dps.dml.client.util.*;
import io.vertx.core.buffer.*;

import java.util.*;

public class GetFreeStorageNodeCommand implements Command<String> {

    private final Set<String> zones;
    private final long objectSizeInBytes;

    public GetFreeStorageNodeCommand(Set<String> zones, long objectSizeInBytes) {
        this.zones = zones;
        this.objectSizeInBytes = objectSizeInBytes;
    }

    @Override
    public CommandType getCommandType() {
        return MetadataCommandType.GET_FREE_STORAGE_NODES;
    }

    @Override
    public void encode(Buffer buffer) {
        buffer.appendLong(objectSizeInBytes);
        buffer.appendInt(zones.size());
        for (String zone : zones) {
            buffer.appendInt(zone.length()).appendString(zone);
        }
    }

    @Override
    public String decodeReply(ReadableBuffer buffer) {
        return buffer.getLengthPrefixedString();
    }
}
