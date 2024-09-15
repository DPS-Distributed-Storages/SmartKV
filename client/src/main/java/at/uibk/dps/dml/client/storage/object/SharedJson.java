package at.uibk.dps.dml.client.storage.object;

import at.uibk.dps.dml.client.DmlClient;
import io.vertx.core.Future;

import java.util.Map;

public class SharedJson extends SharedObject {

    public SharedJson(DmlClient client, String name) {
        super(client, name);
    }

    /**
     * Sets the JSON document to the given value.
     *
     * @param value the value to be set
     */
    public Future<Void> set(Object value) {
        return set(new Object[]{value}, false).mapEmpty();
    }

    /**
     * Sets the value at the given path.
     *
     * @param path the JSONPath to the value to be set
     * @param value the value to be set
     */
    public Future<Void> set(String path, Object value) {
        return invokeMethod("set", new Object[]{path, value}, false, false).mapEmpty();
    }

    /**
     * Returns the JSON document.
     *
     * @return the JSON document
     */
    public Future<Object> get() {
        return get(false).map(Map.class::cast).map(res -> res.get("json"));
    }

    /**
     * Returns the value at the given path.
     *
     * @param path the JSONPath to the value to be returned
     * @return the value at the given path
     */
    public Future<Object> get(String path) {
        return invokeMethod("get", new Object[]{path}, true, false);
    }

    /**
     * Adds or updates the key with the given value.
     *
     * @param key the key to add or update
     * @param value the value to add or update
     */
    public Future<Void> put(String key, Object value) {
        return invokeMethod("put", new Object[]{"$", key, value}, false, false).mapEmpty();
    }

    /**
     * Adds or updates the key with the given value at the given path.
     *
     * @param path the JSONPath to the key
     * @param key the key to add or update
     * @param value the value to add or update
     */
    public Future<Void> put(String path, String key, Object value) {
        return invokeMethod("put", new Object[]{path, key, value}, false, false).mapEmpty();
    }

    /**
     * Adds the given value to the array at the given path.
     *
     * @param path the JSONPath to the array
     * @param value the value to add
     */
    public Future<Void> add(String path, Object value) {
        return invokeMethod("add", new Object[]{path, value}, false, false).mapEmpty();
    }

    /**
     * Deletes the value at the given path.
     *
     * @param path the JSONPath to the value to be deleted
     */
    public Future<Void> delete(String path) {
        return invokeMethod("delete", new Object[]{path}, false, false).mapEmpty();
    }
}
