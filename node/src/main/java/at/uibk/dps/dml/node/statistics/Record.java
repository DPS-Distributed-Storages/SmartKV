package at.uibk.dps.dml.node.statistics;

import java.util.HashMap;
import java.util.Map;

public class Record {

    private final long timestamp;

    private final Map<StatisticKey, StatisticEntry> statistics;

    public Record() {
        this.timestamp = System.currentTimeMillis();
        this.statistics = new HashMap<>();
    }

    public Record(long timestamp, Map<StatisticKey, StatisticEntry> statistics) {
        this.timestamp = timestamp;
        this.statistics = statistics;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<StatisticKey, StatisticEntry> getStatistics() {
        return statistics;
    }
}
