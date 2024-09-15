package at.uibk.dps.dml.client.metadata.commands;

import at.uibk.dps.dml.client.Command;
import at.uibk.dps.dml.client.CommandType;
import at.uibk.dps.dml.client.metadata.MetadataCommandType;
import at.uibk.dps.dml.client.util.ReadableBuffer;
import io.vertx.core.buffer.Buffer;

public class GetMembershipViewCommand implements Command<String> {

    @Override
    public CommandType getCommandType() {
        return MetadataCommandType.GET_MEMBERSHIP_VIEW;
    }

    @Override
    public void encode(Buffer buffer) {
        // Nothing to encode
    }

    @Override
    public String decodeReply(ReadableBuffer buffer) {
        return buffer.getLengthPrefixedString();
    }
}
