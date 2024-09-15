package at.uibk.dps.dml.node.storage.object;

import java.io.Serializable;

/**
 * The {@link SharedClassDef} holds a user-defined class definition as byte-code.
 */
public class SharedClassDef implements Serializable {

    private byte[] byte_code;
    private String class_name;
    private String language;

    /**
     * Default constructor that initializes the buffer with {@code null}.
     */
    public SharedClassDef() {
    }

    /**
     * Constructs a buffer with the given byte-code and sets the class name.
     *
     * @param byte_code the byte-code
     * @param class_name the class name
     */
    public SharedClassDef(byte[] byte_code, String class_name, String language) {
        this.byte_code = byte_code;
        this.class_name = class_name;
        this.language = language;
    }

    /**
     * Sets the byte-code buffer to the given value and the class name to the given value
     *
     * @param byte_code the byte-code to be set
     * @param class_name the name of the class to be set
     */
    public void set(byte[] byte_code, String class_name) {
        this.byte_code = byte_code;
        this.class_name = class_name;
        this.language = language;
    }

    /**
     * Returns this SharedClassDef object.
     *
     * @return this object
     */
   public SharedClassDef get() {
        return this;
    }

    /**
     * Returns the value of the byte-code buffer.
     *
     * @return the value of the byte-code buffer
     */
    public byte[] getByteCode() {
        return this.byte_code;
    }

    /**
     * Returns the value of the class_name.
     *
     * @return the value of the class_name
     */
    public String getClassName() {
        return this.class_name;
    }

    /**
     * Returns the language of this class definition.
     *
     * @return the language of this class definition
     */
    public String getLanguage() {
        return this.language;
    }

}
