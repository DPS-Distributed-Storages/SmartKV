package at.uibk.dps.dml.node.storage.command;

import at.uibk.dps.dml.client.util.BufferUtil;
import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;

public class SetCommand extends InvalidationCommand {

    private static final InvalidationCommandType commandType = InvalidationCommandType.SET;

    private byte[] encodedArgs;

    public SetCommand() {
    }

    public SetCommand(Promise<Object> promise, String key, Integer lockToken, int originVerticleId,
                      boolean asyncReplication, byte[] encodedArgs) {
        super(promise, key, lockToken, false, false, originVerticleId, asyncReplication);
        this.encodedArgs = encodedArgs;
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
        if (encodedArgs != null) {
            buffer.appendInt(encodedArgs.length).appendBytes(encodedArgs);
        } else {
            buffer.appendInt(-1);
        }
    }

    @Override
    protected void decodePayload(BufferReader bufferReader) {
        int encodedArgsLength = bufferReader.getInt();
        encodedArgs = encodedArgsLength != -1 ?  bufferReader.getBytes(encodedArgsLength) : null;
        readOnly = false;
    }
}
