package at.uibk.dps.dml.node.storage.object;

import java.io.Serializable;

/**
 * The {@link SharedBuffer} holds a byte array.
 */
public class SharedBuffer implements Serializable {

    private byte[] buffer;

    /**
     * Default constructor that initializes the buffer with {@code null}.
     */
    public SharedBuffer() {
    }

    /**
     * Constructs a buffer with the given byte array.
     *
     * @param buffer the byte array
     */
    public SharedBuffer(byte[] buffer) {
        this.buffer = buffer;
    }

    /**
     * Getter for the buffer
     * @return the buffer
     */
    public byte[] getBuffer() {
        return buffer;
    }

    /**
     * Sets the buffer to the given value
     *
     * @param buffer the value to be set
     */


    public void set(byte[] buffer) {
        this.buffer = buffer;
    }

    /**
     * Returns the value of the buffer.
     *
     * @return the value of the buffer
     */
    public byte[] get() {
        return this.buffer;
    }
}
