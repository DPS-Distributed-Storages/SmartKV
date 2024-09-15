package at.uibk.dps.dml.client.storage.object;

import at.uibk.dps.dml.client.DmlClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.Map;

public class SharedStatistics extends SharedObject {

    public SharedStatistics(DmlClient client, String key) {
        super(client, key);
    }

    /**
     * Returns the current statistics.
     *
     * @return a future that completes with the current statistics
     */
    public Future<ArrayList> get() {
        return get(false).map(Map.class::cast).map(res -> res.get("statistics")).map(ArrayList.class::cast);
    }

    /**
     * Adds a value to the statistics.
     *
     * @return a future that completes with the success of adding the value
     */
    public Future<Boolean> add(long value) {
        return invokeMethod("add", new Object[]{value}, false, false).map(Boolean.class::cast);
    }

    /**
     * Removes the first occurence of specified value from the statistics.
     *
     * @return a future that completes with the removal of the value
     */
    public Future<Boolean> remove(long value) {
        return invokeMethod("remove", new Object[]{value}, false, false).map(Boolean.class::cast);
    }

    /**
     * Returns the average over all statistics.
     *
     * @return a future that completes with the average of the statistics
     */
    public Future<Double> average() {
        return invokeMethod("average", null, true, false).map(Double.class::cast);
    }

}
