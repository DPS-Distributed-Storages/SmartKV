package at.uibk.dps.dml.client.storage.commands;

import at.uibk.dps.dml.client.Command;
import at.uibk.dps.dml.client.CommandType;
import at.uibk.dps.dml.client.storage.Response;
import at.uibk.dps.dml.client.storage.SharedObjectArgsCodec;
import at.uibk.dps.dml.client.storage.StorageCommandType;
import at.uibk.dps.dml.client.util.BufferUtil;
import at.uibk.dps.dml.client.util.ReadableBuffer;
import io.vertx.core.buffer.Buffer;

public class InitObjectCommand implements Command<Response<Void>> {

    private final SharedObjectArgsCodec argsCodec;

    private final String key;

    private final String languageId;

    private final String objectType;

    private final Object[] args;

    private final Integer lockToken;

    public InitObjectCommand(SharedObjectArgsCodec argsCodec, String key,
                             String languageId, String objectType, Object[] args, Integer lockToken) {
        this.argsCodec = argsCodec;
        this.key = key;
        this.languageId = languageId;
        this.objectType = objectType;
        this.args = args;
        this.lockToken = lockToken;
    }

    @Override
    public CommandType getCommandType() {
        return StorageCommandType.INIT_OBJECT;
    }

    @Override
    public void encode(Buffer buffer) {
        BufferUtil.appendLengthPrefixedString(buffer, key);
        BufferUtil.appendLengthPrefixedString(buffer, languageId);
        BufferUtil.appendLengthPrefixedString(buffer, objectType);
        if (args == null) {
            buffer.appendInt(-1);
        } else {
            byte[] encodedArgs = argsCodec.encode(args);
            buffer.appendInt(encodedArgs.length).appendBytes(encodedArgs);
        }
        if (lockToken != null) {
            buffer.appendInt(lockToken);
        }
    }

    @Override
    public Response<Void> decodeReply(ReadableBuffer buffer) {
        int metadataVersion = buffer.getInt();
        return new Response<>(metadataVersion, null);
    }
}
