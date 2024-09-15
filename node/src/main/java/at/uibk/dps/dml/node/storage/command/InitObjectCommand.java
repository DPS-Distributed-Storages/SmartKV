package at.uibk.dps.dml.node.storage.command;

import at.uibk.dps.dml.client.util.BufferUtil;
import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;

public class InitObjectCommand extends InvalidationCommand {

    private static final InvalidationCommandType commandType = InvalidationCommandType.INIT_OBJECT;

    private String languageId;

    private String objectType;

    private byte[] encodedArgs;

    public InitObjectCommand() {
    }

    public InitObjectCommand(Promise<Object> promise, String key, Integer lockToken, int originVerticleId,
                             String languageId, String objectType, byte[] encodedArgs) {
        super(promise, key, lockToken, false, false, originVerticleId, false);
        this.languageId = languageId;
        this.objectType = objectType;
        this.encodedArgs = encodedArgs;
    }

    public String getLanguageId() {
        return languageId;
    }

    public String getObjectType() {
        return objectType;
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
        BufferUtil.appendLengthPrefixedString(buffer, languageId);
        BufferUtil.appendLengthPrefixedString(buffer, objectType);
        if (encodedArgs != null) {
            buffer.appendInt(encodedArgs.length).appendBytes(encodedArgs);
        } else {
            buffer.appendInt(-1);
        }
    }

    @Override
    protected void decodePayload(BufferReader bufferReader) {
        languageId = bufferReader.getLengthPrefixedString();
        objectType = bufferReader.getLengthPrefixedString();
        int encodedArgsLength = bufferReader.getInt();
        encodedArgs = encodedArgsLength != -1 ?  bufferReader.getBytes(encodedArgsLength) : null;
    }
}
