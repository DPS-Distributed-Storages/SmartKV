package at.uibk.dps.dml.node.storage;

/**
 * The {@link StorageObjectState} is used to indicate the replication state of a storage object.
 */
public enum StorageObjectState {

    /**
     * The object is not yet created.
     */
    CREATE,

    /**
     * The object is valid and has no uncommitted changes.
     */
    VALID,

    /**
     * The object has uncommitted changes.
     */
    INVALID

}
