package at.uibk.dps.dml.node.statistics;

import java.util.Objects;

/**
 * This class is a wrapper for combining a client zone and a KV object's key into a single object.
 * This object is then used in the StatisticManager as 2D index to the map containing all statistics
 */
public class StatisticKey {

    private final String key;

    private final String clientZone;

    public StatisticKey(String key, String clientZone) {
        this.key = key;
        this.clientZone = clientZone;
    }

    public String getKey() {
        return key;
    }

    public String getClientZone() {
        return clientZone;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatisticKey that = (StatisticKey) o;
        return Objects.equals(key, that.key) && Objects.equals(clientZone, that.clientZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, clientZone);
    }
}
