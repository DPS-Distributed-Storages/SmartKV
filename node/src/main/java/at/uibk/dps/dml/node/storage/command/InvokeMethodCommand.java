package at.uibk.dps.dml.node.storage.command;

import at.uibk.dps.dml.client.util.BufferUtil;
import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;

public class InvokeMethodCommand extends InvalidationCommand {

    private static final InvalidationCommandType commandType = InvalidationCommandType.INVOKE_METHOD;

    private String methodName;

    private byte[] encodedArgs;

    public InvokeMethodCommand() {
    }

    public InvokeMethodCommand(Promise<Object> promise, String key, Integer lockToken,
                               boolean readOnly, boolean allowInvalidReads,
                               int originVerticleId, boolean asyncReplication,
                               String methodName, byte[] encodedArgs) {
        super(promise, key, lockToken, readOnly, allowInvalidReads, originVerticleId, asyncReplication);
        this.methodName = methodName;
        this.encodedArgs = encodedArgs;
    }

    public String getMethodName() {
        return methodName;
    }

    public byte[] getEncodedArgs() {
        return encodedArgs;
    }

    @Override
    public Object apply(CommandHandler handler) {
        return handler.apply(this);
    }

    @Override
    public InvalidationCommandType getType() {
        return commandType;
    }

    @Override
    protected void encodePayload(Buffer buffer) {
        BufferUtil.appendLengthPrefixedString(buffer, methodName);
        if (encodedArgs != null) {
            buffer.appendInt(encodedArgs.length).appendBytes(encodedArgs);
        } else {
            buffer.appendInt(-1);
        }
    }

    @Override
    protected void decodePayload(BufferReader bufferReader) {
        methodName = bufferReader.getLengthPrefixedString();
        int encodedArgsLength = bufferReader.getInt();
        encodedArgs = encodedArgsLength != -1 ?  bufferReader.getBytes(encodedArgsLength) : null;
    }
}
