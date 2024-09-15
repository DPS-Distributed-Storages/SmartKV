package at.uibk.dps.dml.node.storage.rpc;

/**
 * The {@link StorageRpcType} enum contains the different types of RPCs that can be sent to remote storage verticles.
 * The ordinal of each enum value is used to encode the type in the RPC message.
 */
public enum StorageRpcType {

    /**
     * An invalidation request.
     */
    INVALIDATE,

    /**
     * A commit request.
     */
    COMMIT,

    /**
     * A request for an object.
     */
    GET_OBJECT,


    /**
     * A request for the free memory.
     */
    GET_FREE_MEMORY,

}
