package at.uibk.dps.dml.client.storage;

import at.uibk.dps.dml.client.CommandType;

import java.util.Arrays;

public enum StorageCommandType implements CommandType {

    LOCK(0),

    UNLOCK(1),

    INIT_OBJECT(2),

    INVOKE_METHOD(3),

    SET(4),

    GET(5),

    PUSH_CLIENT_LOCATION(6);

    private final byte id;

    StorageCommandType(int id) {
        this.id = (byte) id;
    }

    @Override
    public byte getId() {
        return id;
    }

    public static StorageCommandType valueOf(int id) {
        return Arrays.stream(StorageCommandType.values())
                .filter(value -> value.id == id)
                .findFirst()
                .orElse(null);
    }
}
