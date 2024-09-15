package at.uibk.dps.dml.client.storage.commands;

import at.uibk.dps.dml.client.Command;
import at.uibk.dps.dml.client.CommandType;
import at.uibk.dps.dml.client.storage.Response;
import at.uibk.dps.dml.client.storage.StorageCommandType;
import at.uibk.dps.dml.client.util.BufferUtil;
import at.uibk.dps.dml.client.util.ReadableBuffer;
import io.vertx.core.buffer.Buffer;

public class UnlockCommand implements Command<Response<Void>> {

    private final String key;
    private final int lockToken;

    public UnlockCommand(String key, int lockToken) {
        this.key = key;
        this.lockToken = lockToken;
    }

    @Override
    public CommandType getCommandType() {
        return StorageCommandType.UNLOCK;
    }

    @Override
    public void encode(Buffer buffer) {
        BufferUtil.appendLengthPrefixedString(buffer, key);
        buffer.appendInt(lockToken);
    }

    @Override
    public Response<Void> decodeReply(ReadableBuffer buffer) {
        int metadataVersion = buffer.getInt();
        return new Response<>(metadataVersion, null);
    }
}
