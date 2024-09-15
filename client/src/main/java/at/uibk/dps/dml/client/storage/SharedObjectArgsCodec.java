package at.uibk.dps.dml.client.storage;

import java.io.Serializable;

/**
 * Encodes and decodes the arguments of a shared object.
 */
public interface SharedObjectArgsCodec extends Serializable {

    /**
     * Encodes the given arguments into a byte array.
     *
     * @param args the arguments
     * @return the encoded arguments
     */
    byte[] encode(Object[] args);

    /**
     * Decodes the given byte array into arguments.
     *
     * @param encodedArgs the encoded arguments
     * @return the decoded arguments
     */
    Object[] decode(byte[] encodedArgs);

}
