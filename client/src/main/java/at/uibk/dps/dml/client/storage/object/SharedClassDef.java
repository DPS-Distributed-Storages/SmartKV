package at.uibk.dps.dml.client.storage.object;

import at.uibk.dps.dml.client.DmlClient;
import io.vertx.core.Future;

public class SharedClassDef extends SharedObject {

    public SharedClassDef(DmlClient client, String key) {
        super(client, key);
    }

    /**
     * Returns the current value of the buffer.
     *
     * @return a future that completes with the current value of the buffer
     */
    public Future<Object> get() {
        return get(false);
    }

    /**
     * Sets the byte-code buffer to the given value and the class name to the given class name
     *
     * @return a future that completes when the value has been set
     */
    public Future<Void> set(String class_name, byte[] value) {
        return set(new Object[]{class_name, value}, false).mapEmpty();
    }

    /**
     * Returns the current value of the byte-code buffer.
     *
     * @return a future that completes with the current value of the buffer
     */
    public Future<byte[]> getByteCode() {
        return invokeMethod("getByteCode", null, true, false).map(byte[].class::cast);
    }

    /**
     * Returns the current class name.
     *
     * @return a future that completes with the current class name
     */
    public Future<String> getClassName() {
        return invokeMethod("getClassName", null, true, false).map(String.class::cast);
    }


}
