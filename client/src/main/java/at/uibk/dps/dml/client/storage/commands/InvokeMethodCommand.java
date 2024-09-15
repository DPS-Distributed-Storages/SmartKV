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

public class InvokeMethodCommand implements Command<Response<Object>> {

    private final SharedObjectArgsCodec argsCodec;

    private final String key;

    private final String methodName;

    private final Object[] args;

    private final Integer lockToken;

    private final Set<Flag> flags;

    public InvokeMethodCommand(SharedObjectArgsCodec argsCodec, String key, String methodName, Object[] args,
                               Integer lockToken, Set<Flag> flags) {
        this.argsCodec = argsCodec;
        this.key = key;
        this.methodName = methodName;
        this.args = args;
        this.lockToken = lockToken;
        this.flags = flags;
    }

    @Override
    public CommandType getCommandType() {
        return StorageCommandType.INVOKE_METHOD;
    }

    @Override
    public void encode(Buffer buffer) {
        BufferUtil.appendLengthPrefixedString(buffer, key);
        BufferUtil.appendLengthPrefixedString(buffer, methodName);
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
    public Response<Object> decodeReply(ReadableBuffer buffer) {
        int metadataVersion = buffer.getInt();
        Object result = null;
        int resultLength = buffer.getInt();
        if (resultLength > 0) {
            result = argsCodec.decode(buffer.getBytes(resultLength))[0];
        }
        return new Response<>(metadataVersion, result);
    }
}
