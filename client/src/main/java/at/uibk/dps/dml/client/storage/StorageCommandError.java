package at.uibk.dps.dml.client.storage;

public class StorageCommandError extends Throwable {

    private final StorageCommandErrorType storageCommandErrorType;

    public StorageCommandError(StorageCommandErrorType storageCommandErrorType, String message) {
        super("Command failed: " + (message != null && !message.isEmpty() ? message : storageCommandErrorType));
        this.storageCommandErrorType = storageCommandErrorType;
    }

    public StorageCommandErrorType getErrorType() {
        return storageCommandErrorType;
    }
}
