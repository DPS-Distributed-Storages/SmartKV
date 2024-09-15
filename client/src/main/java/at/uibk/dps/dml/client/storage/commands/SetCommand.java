package at.uibk.dps.dml.client.storage.commands;

import at.uibk.dps.dml.client.Command;
import at.uibk.dps.dml.client.CommandType;
import at.uibk.dps.dml.client.storage.Flag;
import at.uibk.dps.dml.client.storage.Response;
import at.uibk.dps.dml.client.storage.SharedObjectArgsCodec;
import at.uibk.dps.dml.client.storage.StorageCommandType;
import at.uibk.dps.dml.client.util.BufferUtil;
import at.uibk.dps.dml.client.util.ReadableBuffer;
import io.vertx.core.buffer.Buffer;

import java.util.Set;

public class SetCommand implements Command<Response<Void>> {

    private final SharedObjectArgsCodec argsCodec;

    private final String key;

    private final Object[] args;

    private final Integer lockToken;

    private final Set<Flag> flags;

    public SetCommand(SharedObjectArgsCodec argsCodec, String key,
                      Object[] args, Integer lockToken, Set<Flag> flags) {
        this.argsCodec = argsCodec;
        this.key = key;
        this.args = args;
        this.lockToken = lockToken;
        this.flags = flags;
    }

    @Override
    public CommandType getCommandType() {
        return StorageCommandType.SET;
    }

    @Override
    public void encode(Buffer buffer) {
        BufferUtil.appendLengthPrefixedString(buffer, key);
        if (args == null) {
            buffer.appendInt(-1);
        } else {
            byte[] encodedArgs = argsCodec.encode(args);
            buffer.appendInt(encodedArgs.length).appendBytes(encodedArgs);
        }
        byte flagsBitVector = 0;
        for (Flag flag : flags) {
            flagsBitVector |= flag.getValue();
        }
        buffer.appendByte(flagsBitVector);
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
