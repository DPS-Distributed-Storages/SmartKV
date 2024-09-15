package at.uibk.dps.dml.client.storage.commands;

import at.uibk.dps.dml.client.Command;
import at.uibk.dps.dml.client.CommandType;
import at.uibk.dps.dml.client.storage.Response;
import at.uibk.dps.dml.client.storage.StorageCommandType;
import at.uibk.dps.dml.client.util.BufferUtil;
import at.uibk.dps.dml.client.util.ReadableBuffer;
import io.vertx.core.buffer.Buffer;

public class LockCommand implements Command<Response<Integer>> {

    private final String key;

    public LockCommand(String key) {
        this.key = key;
    }

    @Override
    public CommandType getCommandType() {
        return StorageCommandType.LOCK;
    }

    @Override
    public void encode(Buffer buffer) {
        BufferUtil.appendLengthPrefixedString(buffer, key);
    }

    @Override
    public Response<Integer> decodeReply(ReadableBuffer buffer) {
        int metadataVersion = buffer.getInt();
        int lockToken = buffer.getInt();
        return new Response<>(metadataVersion, lockToken);
    }
}
