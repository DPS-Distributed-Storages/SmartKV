package at.uibk.dps.dml.client.storage;

import java.util.Arrays;

public enum StorageCommandErrorType {

    UNKNOWN_ERROR(0, "An unknown error occurred"),

    UNKNOWN_COMMAND(1, "Unknown command"),

    KEY_DOES_NOT_EXIST(2, "Key does not exist"),

    INVALID_LOCK_TOKEN(3, "Invalid lock token"),

    OBJECT_NOT_INITIALIZED(4, "Object not initialized"),

    NOT_RESPONSIBLE(5, "Not responsible for this key"),

    SHARED_OBJECT_ERROR(6, "Shared object operation failed");

    private final int errorCode;

    private final String message;

    StorageCommandErrorType(int errorCode, String message) {
        this.errorCode = errorCode;
        this.message = message;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public static StorageCommandErrorType valueOf(int errorCode) {
        return Arrays.stream(StorageCommandErrorType.values())
                .filter(value -> value.errorCode == errorCode)
                .findFirst()
                .orElse(null);
    }

    @Override
    public String toString() {
        return "StorageCommandErrorType{" +
                "errorCode=" + errorCode +
                ", message='" + message + '\'' +
                '}';
    }
}
