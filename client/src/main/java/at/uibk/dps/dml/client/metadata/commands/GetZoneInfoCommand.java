package at.uibk.dps.dml.client.metadata.commands;

import at.uibk.dps.dml.client.Command;
import at.uibk.dps.dml.client.CommandType;
import at.uibk.dps.dml.client.metadata.MetadataCommandType;
import at.uibk.dps.dml.client.util.ReadableBuffer;
import io.vertx.core.buffer.Buffer;

public class GetZoneInfoCommand implements Command<String> {

    private final String zone;

    public GetZoneInfoCommand(String zone) {
        this.zone = zone;
    }

    @Override
    public CommandType getCommandType() {
        return MetadataCommandType.GET_ZONE_INFO;
    }

    @Override
    public void encode(Buffer buffer) {
        buffer.appendInt(zone.length()).appendString(zone);
    }

    @Override
    public String decodeReply(ReadableBuffer buffer) {
        return buffer.getLengthPrefixedString();
    }
}
