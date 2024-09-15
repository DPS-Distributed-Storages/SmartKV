package at.uibk.dps.dml.node.exception;

import at.uibk.dps.dml.node.storage.rpc.StorageRpcErrorType;

public class StorageRpcException extends RuntimeException {

    private final StorageRpcErrorType storageRpcErrorType;

    public StorageRpcException(StorageRpcErrorType storageRpcErrorType, String message) {
        super(message);
        this.storageRpcErrorType = storageRpcErrorType;
    }

    public StorageRpcErrorType getErrorType() {
        return storageRpcErrorType;
    }
}
