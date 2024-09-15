package at.uibk.dps.dml.node.exception;

import at.uibk.dps.dml.node.metadata.rpc.MetadataRpcErrorType;

public class MetadataRpcException extends RuntimeException {

    private final MetadataRpcErrorType metadataRpcErrorType;

    public MetadataRpcException(MetadataRpcErrorType metadataRpcErrorType, String message) {
        super(message);
        this.metadataRpcErrorType = metadataRpcErrorType;
    }

    public MetadataRpcErrorType getErrorType() {
        return metadataRpcErrorType;
    }
}
