package at.uibk.dps.dml.node.storage.rpc;

/**
 * The {@link StorageRpcErrorType} enum contains the different types of errors that can occur during an RPC.
 * The ordinal of each enum value is used to encode the error type in the RPC message.
 */
public enum StorageRpcErrorType {

    UNKNOWN_ERROR,

    TIMEOUT,

    UNKNOWN_COMMAND,

    MISSING_MESSAGES,

    CONFLICTING_TIMESTAMPS,

    REJECTED,

    KEY_DOES_NOT_EXIST,

    OBJECT_NOT_READY,
}
