package at.uibk.dps.dml.client;

import java.util.Arrays;

public enum CommandResultType {

    SUCCESS(0),

    ERROR(1);

    private final byte id;

    CommandResultType(int id) {
        this.id = (byte) id;
    }

    public byte getId() {
        return id;
    }

    public static CommandResultType valueOf(int id) {
        return Arrays.stream(CommandResultType.values())
                .filter(value -> value.id == id)
                .findFirst()
                .orElse(null);
    }
}
