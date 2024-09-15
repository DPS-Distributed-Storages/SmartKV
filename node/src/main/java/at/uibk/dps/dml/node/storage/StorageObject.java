package at.uibk.dps.dml.node.storage;

import at.uibk.dps.dml.node.util.Timestamp;

import java.io.Serializable;

/**
 * The {@link StorageObject} holds a shared object and additional metadata about its state.
 */
public class StorageObject implements Serializable {

    private final Timestamp timestamp;

    private SharedObject sharedObject;

    private StorageObjectState state;

    private boolean locked;

    private int lockToken;

    /**
     * Default constructor.
     *
     * @param timestamp the timestamp of the object
     */
    public StorageObject(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Returns the timestamp of the object.
     *
     * @return the timestamp
     */
    public Timestamp getTimestamp() {
        return timestamp;
    }

    /**
     * Returns the shared object.
     *
     * @return the shared object
     */
    public SharedObject getSharedObject() {
        return sharedObject;
    }

    /**
     * Sets the shared object.
     *
     * @param sharedObject the shared object
     */
    public void setSharedObject(SharedObject sharedObject) {
        this.sharedObject = sharedObject;
    }

    /**
     * Returns true if the object is locked, false otherwise.
     *
     * @return true if the object is locked, false otherwise
     */
    public boolean isLocked() {
        return locked;
    }

    /**
     * Sets the locked flag.
     *
     * @param locked the locked flag
     */
    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    /**
     * Returns the lock token.
     *
     * @return the lock token
     */
    public int getLockToken() {
        return lockToken;
    }

    /**
     * Sets the lock token.
     *
     * @param lockToken the lock token
     */
    public void setLockToken(int lockToken) {
        this.lockToken = lockToken;
    }


    /**
     * Returns the state of the object.
     *
     * @return the state
     */
    public StorageObjectState getState() {
        return state;
    }

    /**
     * Sets the state of the object.
     *
     * @param state the state
     */
    public void setState(StorageObjectState state) {
        this.state = state;
    }

    /**
     * Copies the values of the given object to this object.
     *
     * @param other the object to copy from
     */
    public void copyFrom(StorageObject other) {
        this.timestamp.setVersion(other.getTimestamp().getVersion());
        this.timestamp.setCoordinatorVerticleId(other.getTimestamp().getCoordinatorVerticleId());
        this.sharedObject = other.getSharedObject();
        this.state = other.getState();
        this.locked = other.isLocked();
        this.lockToken = other.getLockToken();
    }

    @Override
    public String toString() {
        return "StorageObject{" +
                "timestamp=" + timestamp +
                ", sharedObject=" + sharedObject +
                ", state=" + state +
                ", locked=" + locked +
                ", lockToken=" + lockToken +
                '}';
    }
}
