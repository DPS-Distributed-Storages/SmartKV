package at.uibk.dps.dml.node.statistics;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class StatisticKeyTest {

    @Test
    void testEquals() {
        StatisticKey key1 = new StatisticKey("key1", "zone1");
        StatisticKey key2 = new StatisticKey("key1", "zone1");
        StatisticKey key3 = new StatisticKey("key2", "zone2");

        assertEquals(key1, key2);
        assertNotEquals(key1, key3);
    }

    @Test
    void testHashCode() {
        StatisticKey key1 = new StatisticKey("key1", "zone1");
        StatisticKey key2 = new StatisticKey("key1", "zone1");
        StatisticKey key3 = new StatisticKey("key2", "zone2");

        assertEquals(key1.hashCode(), key2.hashCode());
        assertNotEquals(key1.hashCode(), key3.hashCode());
    }

    @Test
    void testGetKey() {
        StatisticKey key = new StatisticKey("key", "zone");
        assertEquals("key", key.getKey());
    }

    @Test
    void testGetClientZone() {
        StatisticKey key = new StatisticKey("key", "zone");
        assertEquals("zone", key.getClientZone());
    }
}
