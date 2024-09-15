package at.uibk.dps.dml.client.storage.object;

import at.uibk.dps.dml.client.DmlClient;
import io.vertx.core.Future;

import java.util.Map;

public class SharedCounter extends SharedObject {

    public SharedCounter(DmlClient client, String key) {
        super(client, key);
    }

    /**
     * Returns the current value of the counter.
     *
     * @return a future that completes with the current value of the counter
     */
    public Future<Long> get() {
        return get(false).map(Map.class::cast).map(res -> res.get("value")).map(Long.class::cast);
    }

    /**
     * Sets the counter to the given value.
     *
     * @return a future that completes with the current value of the counter
     */
    public Future<Long> set(long value) {
        return set(new Object[]{value}, false).mapEmpty();
    }

    /**
     * Increments the counter by 1.
     *
     * @return a future that completes with the new value of the counter
     */
    public Future<Long> increment() {
        return increment(1);
    }

    /**
     * Increments the counter by the given delta.
     *
     * @param delta the value to increment the counter by
     * @return a future that completes with the new value of the counter
     */
    public Future<Long> increment(long delta) {
        return invokeMethod("increment", new Object[]{delta}, false, false).map(Long.class::cast);
    }

    /**
     * Decrements the counter by 1.
     *
     * @return a future that completes with the new value of the counter
     */
    public Future<Long> decrement() {
        return decrement(1);
    }

    /**
     * Decrements the counter by the given delta.
     *
     * @param delta the value to decrement the counter by
     * @return a future that completes with the new value of the counter
     */
    public Future<Long> decrement(long delta) {
        return invokeMethod("decrement", new Object[]{delta}, false, false).map(Long.class::cast);
    }
}
