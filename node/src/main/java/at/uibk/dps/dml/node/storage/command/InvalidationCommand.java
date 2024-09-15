package at.uibk.dps.dml.node.storage.command;

import at.uibk.dps.dml.client.util.BufferUtil;
import at.uibk.dps.dml.node.util.BufferReader;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;

/**
 * The {@link InvalidationCommand} is the abstract class for all commands which may have to be replicated.
 */
public abstract class InvalidationCommand extends Command {

    protected int originVerticleId;

    protected boolean asyncReplication;

    /**
     * Default constructor. This is used on the receiving side when the command is replicated. Fields are set via
     * {@link #decode(BufferReader)}.
     */
    protected InvalidationCommand() {
    }

    /**
     * All-args constructor.
     *
     * @param promise the command completion promise
     * @param key the key
     * @param lockToken the lock token
     * @param readOnly whether the command is read-only
     * @param allowInvalidReads whether reads of uncommitted values are allowed
     * @param originVerticleId the ID of the verticle which sent the command
     * @param asyncReplication whether the replication is done asynchronously
     */
    protected InvalidationCommand(Promise<Object> promise, String key, Integer lockToken, boolean readOnly,
                                  boolean allowInvalidReads,
                                  int originVerticleId, boolean asyncReplication) {
        super(promise, key, lockToken, readOnly, allowInvalidReads);
        this.originVerticleId = originVerticleId;
        this.asyncReplication = asyncReplication;
    }

    /**
     * Returns the ID of the verticle which sent the command.
     *
     * @return the ID of the verticle which sent the command
     */
    public int getOriginVerticleId() {
        return originVerticleId;
    }

    /**
     * Returns whether the replication is done asynchronously.
     *
     * @return {@code true} if the replication is done asynchronously, {@code false} otherwise
     */
    public boolean isAsyncReplicationEnabled() {
        return asyncReplication;
    }

    /**
     * Returns the type of the command.
     *
     * @return the type of the command
     */
    public abstract InvalidationCommandType getType();

    /**
     * Encodes the command into the given buffer.
     *
     * @param buffer the target buffer
     */
    public final void encode(Buffer buffer) {
        BufferUtil.appendLengthPrefixedString(buffer, key);
        buffer.appendInt(originVerticleId);
        encodePayload(buffer);
    }

    /**
     * Decodes the command from the given buffer.
     *
     * @param bufferReader the buffer containing the encoded command
     */
    public final void decode(BufferReader bufferReader) {
        key = bufferReader.getLengthPrefixedString();
        originVerticleId = bufferReader.getInt();
        decodePayload(bufferReader);
    }

    /**
     * Encodes the payload of the command into the given buffer.
     *
     * @param buffer the target buffer
     */
    protected abstract void encodePayload(Buffer buffer);

    /**
     * Decodes the payload of the command from the given buffer.
     *
     * @param buffer the buffer containing the encoded payload
     */
    protected abstract void decodePayload(BufferReader buffer);

}
