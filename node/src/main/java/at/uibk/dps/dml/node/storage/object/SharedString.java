package at.uibk.dps.dml.node.storage.object;

import java.io.Serializable;

/**
 * The {@link SharedString} holds a {@link String}.
 */
public class SharedString implements Serializable {

    private String string;

    /**
     * Default constructor that initializes the shared string with {@code null}.
     */
    public SharedString() {
    }

    /**
     * Constructs a {@link SharedString} with the given string as initial value.
     *
     * @param string the initial string
     */
    public SharedString(String string) {
        this.string = string;
    }

    /**
     * Sets the shared string to the given value.
     *
     * @param string the value to be set
     */
    public void set(String string) {
        this.string = string;
    }

    /**
     * Returns the value of the shared string.
     *
     * @return the value of the shared string
     */
    public String get() {
        return this.string;
    }
}
