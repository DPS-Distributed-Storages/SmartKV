package at.uibk.dps.dml.node.statistics.command;

import at.uibk.dps.dml.client.CommandType;

import java.util.Arrays;

public enum OptimizerCommandType implements CommandType {

    GET_STATISTICS(0),

    GET_MAIN_OBJECT_LOCATIONS(1),

    GET_UNIT_PRICES(2),

    GET_BILLS(3),

    CLEAR_STATISTICS(4),

    GET_FREE_MEMORY(5);

    private final byte id;

    OptimizerCommandType(int id) {
        this.id = (byte) id;
    }

    @Override
    public byte getId() {
        return id;
    }

    public static OptimizerCommandType valueOf(int id) {
        return Arrays.stream(OptimizerCommandType.values())
                .filter(value -> value.id == id)
                .findFirst()
                .orElse(null);
    }
}
