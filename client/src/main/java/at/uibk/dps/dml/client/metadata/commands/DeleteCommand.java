package at.uibk.dps.dml.client.metadata.commands;

import at.uibk.dps.dml.client.Command;
import at.uibk.dps.dml.client.CommandType;
import at.uibk.dps.dml.client.metadata.MetadataCommandType;
import at.uibk.dps.dml.client.util.ReadableBuffer;
import io.vertx.core.buffer.Buffer;

public class DeleteCommand implements Command<Void> {

    private final String key;

    public DeleteCommand(String key) {
        this.key = key;
    }

    @Override
    public CommandType getCommandType() {
        return MetadataCommandType.DELETE;
    }

    @Override
    public void encode(Buffer buffer) {
        buffer.appendInt(key.length()).appendString(key);
    }

    @Override
    public Void decodeReply(ReadableBuffer buffer) {
        return null;
    }
}
