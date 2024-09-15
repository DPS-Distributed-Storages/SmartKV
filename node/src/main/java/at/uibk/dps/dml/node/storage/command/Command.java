package at.uibk.dps.dml.node.storage.command;

import io.vertx.core.Promise;

/**
 * The {@link Command} is the abstract class for all storage service commands.
 */
public abstract class Command {

    protected final Promise<Object> promise;

    protected String key;

    protected final Integer lockToken;

    protected boolean readOnly;

    protected boolean allowInvalidReads;

    /**
     * Default constructor.
     */
    protected Command() {
        this.promise = null;
        this.lockToken = null;
    }

    /**
     * All-args constructor.
     *
     * @param promise the command completion promise
     * @param key the key
     * @param lockToken the lock token
     * @param readOnly whether the command is read-only
     * @param allowInvalidReads whether reads of uncommitted values are allowed
     */
    protected Command(Promise<Object> promise, String key, Integer lockToken, boolean readOnly, boolean allowInvalidReads) {
        this.promise = promise;
        this.key = key;
        this.lockToken = lockToken;
        this.readOnly = readOnly;
        this.allowInvalidReads = allowInvalidReads;
    }

    /**
     * Returns the command completion promise.
     *
     * @return the command completion promise
     */
    public Promise<Object> getPromise() {
        return promise;
    }

    /**
     * Returns the key.
     *
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns the lock token.
     *
     * @return the lock token
     */
    public Integer getLockToken() {
        return lockToken;
    }

    /**
     * Returns whether the command is read-only.
     *
     * @return {@code true} if the command is read-only, {@code false} otherwise
     */
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Returns whether reads of uncommitted values are allowed.
     *
     * @return {@code true} if reads of uncommitted values are allowed, {@code false} otherwise
     */
    public boolean isAllowInvalidReadsEnabled() {
        return allowInvalidReads;
    }

    /**
     * Calls the command handler.
     *
     * @param handler the command handler
     * @return the result of the command
     */
    public abstract Object apply(CommandHandler handler);
}
