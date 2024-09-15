package at.uibk.dps.dml.node.metadata.command;

import io.vertx.core.Promise;

public abstract class Command {

    protected final Promise<Object> promise;

    protected String key;

    protected boolean readOnly;

    protected boolean allowInvalidReads;

    protected Command() {
        this.promise = null;
    }

    protected Command(Promise<Object> promise, String key, boolean readOnly, boolean allowInvalidReads) {
        this.promise = promise;
        this.key = key;
        this.readOnly = readOnly;
        this.allowInvalidReads = allowInvalidReads;
    }

    public Promise<Object> getPromise() {
        return promise;
    }

    public String getKey() {
        return key;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isAllowInvalidReadsEnabled() {
        return allowInvalidReads;
    }

    public abstract Object apply(CommandHandler handler);
}
