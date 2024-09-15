package at.uibk.dps.dml.node.storage;

import at.uibk.dps.dml.node.exception.SharedObjectException;

import java.io.Serializable;

/**
 * The {@link SharedObject} interface describes the methods which must be implemented by a shared object.
 */
public interface SharedObject extends Serializable {

    /**
     * Invokes the method with the given name and the given arguments on the shared object.
     *
     * @param methodName the name of the method to be invoked
     * @param encodedArgs the encoded arguments to be passed to the method
     * @return the encoded result of the method invocation
     */
    byte[] invokeMethod(String methodName, byte[] encodedArgs) throws SharedObjectException;

    /**
     * Sets the value of the shared object to the given arguments.
     * @param encodedArgs the encoded new value of the object
     */
    void set(byte[] encodedArgs) throws SharedObjectException;


    /**
     * Returns the shared object.
     * @return the object
     */
    byte[] get() throws SharedObjectException;

    /**
     * Returns the language of this shared object.
     * @return the object
     */
    String getLanguage();

    Object getObject();

    String getObjectType();

}
