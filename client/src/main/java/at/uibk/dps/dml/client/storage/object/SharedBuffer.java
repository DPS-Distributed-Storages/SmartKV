package at.uibk.dps.dml.client.storage.object;

import at.uibk.dps.dml.client.DmlClient;
import io.vertx.core.Future;

import java.util.Map;

public class SharedBuffer extends SharedObject {

    public SharedBuffer(DmlClient client, String key) {
        super(client, key);
    }

    /**
     * Returns the current value of the buffer.
     *
     * @return a future that completes with the current value of the buffer
     */
    public Future<byte[]> get() {
        return get(false).map(Map.class::cast).map(res -> res.get("buffer")).map(byte[].class::cast);
    }

    /**
     * Sets the buffer to the given value.
     *
     * @return a future that completes when the value has been set
     */
    public Future<Void> set(byte[] value) {
        return set(new Object[]{value}, false).mapEmpty();
    }
}
