package at.uibk.dps.dml.node.storage;

import at.uibk.dps.dml.node.exception.SharedObjectException;
import at.uibk.dps.dml.node.storage.object.SharedClassDef;

/**
 * The {@link SharedObjectFactory} is used to create {@link SharedObject}s.
 */
public interface SharedObjectFactory {

    /**
     * Creates a new {@link SharedObject} with the given arguments.
     *
     * @param languageId the programming language id
     * @param objectType the object type
     * @param encodedArgs the encoded arguments
     * @return the created {@link SharedObject}
     */
    SharedObject createObject(String languageId, String objectType, byte[] encodedArgs) throws SharedObjectException;

    /**
     * Registers a new {@link SharedObject} type with the given arguments.
     * @param clazz the definition of the new SharedObject type
     */
    void registerNewClass(SharedClassDef clazz);

}
