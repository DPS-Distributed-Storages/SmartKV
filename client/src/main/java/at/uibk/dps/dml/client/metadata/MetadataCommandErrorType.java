package at.uibk.dps.dml.client.metadata;

import java.util.Arrays;

public enum MetadataCommandErrorType {

    NO_ERROR(0, ""),

    UNKNOWN_ERROR(1, "An unknown error occurred"),

    UNKNOWN_COMMAND(2, "Unknown command"),

    KEY_DOES_NOT_EXIST(3, "Key does not exist"),

    KEY_ALREADY_EXISTS(4, "Key already exists"),

    INVALID_OPERATION(5, "Invalid operation"),

    CONCURRENT_OPERATION(6, "Aborted due to a concurrent operation to the same key"),

    INVALID_OBJECT_LOCATIONS(7, "Invalid object locations");

    private final int errorCode;

    private final String message;

    MetadataCommandErrorType(int errorCode, String message) {
        this.errorCode = errorCode;
        this.message = message;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public static MetadataCommandErrorType valueOf(int errorCode) {
        return Arrays.stream(MetadataCommandErrorType.values())
                .filter(value -> value.errorCode == errorCode)
                .findFirst()
                .orElse(null);
    }

    @Override
    public String toString() {
        return "MetadataCommandErrorType{" +
                "errorCode=" + errorCode +
                ", message='" + message + '\'' +
                '}';
    }
}
