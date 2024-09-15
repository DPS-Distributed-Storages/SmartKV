package at.uibk.dps.dml.client.storage;

public enum Flag {

    /**
     * Indicates that the command does not have side effects and therefore does not need to be replicated.
     */
    READ_ONLY(0),

    /**
     * If this flag is set, a read request is allowed to return data from an object in invalid state. Only has an
     * effect in combination with the {@link #READ_ONLY} flag.
     */
    ALLOW_INVALID_READS(1),

    /**
     * If this flag is set, the command returns the result before replication is completed.
     */
    ASYNC_REPLICATION(2);

    private final byte value;

    Flag(int bit) {
        this.value = (byte) (1 << bit);
    }

    public byte getValue() {
        return value;
    }
}
