package at.uibk.dps.dml.client.metadata;

import at.uibk.dps.dml.client.CommandType;

import java.util.Arrays;

public enum MetadataCommandType implements CommandType {

    NONE(0),

    CREATE(1),

    GET(2),

    DELETE(3),

    RECONFIGURE(4),

    SYNCHRONIZED_RECONFIGURE(5),

    GETALL(6),

    GET_MEMBERSHIP_VIEW(7),

    GET_ZONE_INFO(8),

    GET_FREE_STORAGE_NODES(9);

    private final byte id;

    MetadataCommandType(int id) {
        this.id = (byte) id;
    }

    @Override
    public byte getId() {
        return id;
    }

    public static MetadataCommandType valueOf(int id) {
        return Arrays.stream(MetadataCommandType.values())
                .filter(value -> value.id == id)
                .findFirst()
                .orElse(null);
    }
}
