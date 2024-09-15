package at.uibk.dps.dml.client.storage.object;

import at.uibk.dps.dml.client.DmlClient;
import at.uibk.dps.dml.client.storage.Flag;
import io.vertx.core.Future;

import java.util.EnumSet;
import java.util.Set;

/**
 * Base class for all shared objects.
 */
public abstract class SharedObject {

    protected final DmlClient client;
    protected final String key;

    protected Integer lockToken = null;

    protected SharedObject(DmlClient client, String key) {
        this.client = client;
        this.key = key;
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
     * Sets the lock token.
     *
     * @param lockToken the lock token
     */
    public void setLockToken(Integer lockToken) {
        this.lockToken = lockToken;
    }

    /**
     * Acquires a lock for the object.
     *
     * @return a future that completes when the lock has been acquired
     */
    public Future<Void> lock() {
        return client.lock(key).onSuccess(token -> lockToken = token).mapEmpty();
    }

    /**
     * Unlocks the object.
     *
     * @return a future that completes when the lock has been released
     */
    public Future<Void> unlock() {
        return client.unlock(key, lockToken).onSuccess(v -> lockToken = null).mapEmpty();
    }

    /**
     * Deletes the object.
     *
     * @return a future that completes when the object has been deleted
     */
    public Future<Void> delete() {
        return client.delete(key).mapEmpty();
    }

    /**
     * Invokes a method on the object.
     *
     * @param methodName the name of the method to invoke
     * @param args the arguments to be provided to the method
     * @param readOnly whether the method is read-only
     * @param eventualConsistency if {@code true}, the command is executed in eventual consistency mode instead of
     *                            strong consistency
     *
     * @return a future that completes with the result of the method invocation
     */
    protected Future<Object> invokeMethod(String methodName, Object[] args, boolean readOnly,
                                          boolean eventualConsistency) {
        Set<Flag> flags;
        if (readOnly) {
            flags = eventualConsistency
                    ? EnumSet.of(Flag.READ_ONLY, Flag.ALLOW_INVALID_READS)
                    : EnumSet.of(Flag.READ_ONLY);
        } else if (eventualConsistency) {
            flags = EnumSet.of(Flag.ASYNC_REPLICATION);
        } else {
            flags = EnumSet.noneOf(Flag.class);
        }
        return client.invokeMethod(key, methodName, args, lockToken, flags);
    }

    /**
     * Sets the remote object.
     *
     * @param args the arguments to be provided to the method
     * @param eventualConsistency if {@code true}, the command is executed in eventual consistency mode instead of
     *                            strong consistency
     *
     * @return a Future that succeeds when the SET operation was successful
     */
    protected Future<Void> set(Object[] args, boolean eventualConsistency) {
        return client.set(key, args, lockToken, eventualConsistency);
    }

    /**
     * Gets the remote object.
     *
     * @param allowInvalidReads if {@code true}, the object will also be read if it is currently in INVALID state.
     *
     * @return a Future containing the object
     */
    protected Future<Object> get(boolean allowInvalidReads) {
        return client.get(key, lockToken, allowInvalidReads);
    }


}
