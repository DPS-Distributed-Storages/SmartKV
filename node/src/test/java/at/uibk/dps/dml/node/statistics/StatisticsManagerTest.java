package at.uibk.dps.dml.node.statistics;

import at.uibk.dps.dml.node.billing.TCPRequestType;
import at.uibk.dps.dml.node.util.RandomUtil;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({VertxExtension.class})
public class StatisticsManagerTest {

    private StatisticManager statisticManager;
    private ArrayList<String> zones;
    private ArrayList<String> keys;

    private ArrayList<StatisticKey> statisticKeys;

    @BeforeEach
    public void init() {
        statisticManager = new StatisticManager(null, 1000, 10);
        zones = new ArrayList<>();
        keys = new ArrayList<>();
        statisticKeys = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            String clientZone = "Zone" + i;
            String key = "Key" + i;
            zones.add(clientZone);
            keys.add(key);
            if (new Random().nextBoolean()) {
                statisticKeys.add(new StatisticKey(key, clientZone));
            }
        }

        if (statisticKeys.isEmpty()) {
            statisticKeys.add(new StatisticKey(keys.get(1), zones.get(0)));
            statisticKeys.add(new StatisticKey(keys.get(3), zones.get(2)));
        }

    }

    @Test
    public void testInitialization() {
        assertNotNull(statisticManager.getStatistics());
        assertEquals(0, statisticManager.getStatistics().size());
    }

    @Test
    public void testResetToZero() {
        statisticManager.addStatistic(TCPRequestType.PUT, 0,0, "key1", "zone1");
        statisticManager.addStatistic(TCPRequestType.PUT, 0,0, "key2", "zone2");
        statisticManager.addStatistic(TCPRequestType.PUT, 100,130, "key3", "zone3");
        statisticManager.addStatistic(TCPRequestType.GET, 0,0, "key1", "zone1");
        statisticManager.addStatistic(TCPRequestType.GET, 0,0, "key2", "zone2");
        statisticManager.addStatistic(TCPRequestType.GET, 10,200, "key3", "zone3");

        assertNotEquals(0, statisticManager.getStatistics().size());

        statisticManager.resetAllToZero();

        assertEquals(0, statisticManager.getStatistics().size());

    }

    @Test
    public void testAddPutRequests() {
        testAddRequests(TCPRequestType.PUT);
    }

    @Test
    public void testAddGetRequests() {
        testAddRequests(TCPRequestType.GET);
    }

    @Test
    public void testAddCumulativeMessageSizeSentPUT() {
        testAddCumulativeMessageSize(TCPRequestType.PUT, true, false);
        statisticManager.resetAllToZero();
        testAddCumulativeMessageSize(TCPRequestType.PUT, false, false);

    }

    @Test
    public void testAddCumulativeMessageSizeSentGET() {
        testAddCumulativeMessageSize(TCPRequestType.GET, true, false);
        statisticManager.resetAllToZero();
        testAddCumulativeMessageSize(TCPRequestType.GET, false, false);

    }

    @Test
    public void testAddCumulativeMessageSizeReceivedPUT() {
        testAddCumulativeMessageSize(TCPRequestType.PUT, true, true);
        statisticManager.resetAllToZero();
        testAddCumulativeMessageSize(TCPRequestType.PUT, false, true);
    }

    @Test
    public void testAddCumulativeMessageSizeReceivedGET() {
        testAddCumulativeMessageSize(TCPRequestType.GET, true, true);
        statisticManager.resetAllToZero();
        testAddCumulativeMessageSize(TCPRequestType.GET, false, true);
    }

    public void testAddRequests(TCPRequestType requestType) {

        assertEquals(0, statisticManager.getStatistics().size());

        HashMap<StatisticKey, Long> requestsPerStatKey = addRequests(requestType);

        for (String key : keys) {
            for (String zone : zones) {
                StatisticKey statKey = new StatisticKey(key, zone);
                if (statisticKeys.contains(statKey)) {
                    assertEquals(requestsPerStatKey.get(statKey), statisticManager.getStatistics().get(statKey).getNumberOfRequests().get(requestType));
                } else {
                    assertThrows(NullPointerException.class, () -> statisticManager.getStatistics().get(statKey).getNumberOfRequests().get(requestType));
                }
            }
        }
    }

    public void testAddCumulativeMessageSize(TCPRequestType requestType, boolean testMethodSignature1, boolean isRead) {

        assertEquals(0, statisticManager.getStatistics().size());

        HashMap<StatisticKey, Long> messageSizesPerStatKey;

        if (testMethodSignature1)
            messageSizesPerStatKey = addCumulativeMessageSize(requestType, isRead);
        else
            messageSizesPerStatKey = addCumulativeMessageSize2(requestType, isRead);

        for (String key : keys) {
            for (String zone : zones) {
                StatisticKey statKey = new StatisticKey(key, zone);
                Map<StatisticKey, StatisticEntry> statistics = statisticManager.getStatistics();
                if (statisticKeys.contains(statKey)) {
                    if (isRead) {
                        assertEquals(messageSizesPerStatKey.get(statKey), statistics.get(statKey).getCumulativeMessageSizeReceived().get(requestType));
                    } else {
                        assertEquals(messageSizesPerStatKey.get(statKey), statistics.get(statKey).getCumulativeMessageSizeSent().get(requestType));
                    }
                } else {
                    assertThrows(NullPointerException.class, () -> statistics.get(statKey).getCumulativeMessageSizeReceived().get(requestType));
                    assertThrows(NullPointerException.class, () -> statistics.get(statKey).getCumulativeMessageSizeSent().get(requestType));
                }
            }
        }
    }

    public HashMap<StatisticKey, Long> addRequests(TCPRequestType requestType) {

        HashMap<StatisticKey, Long> requestsPerStatKey = new HashMap<>();

        for (StatisticKey statKey : statisticKeys) {
            long requests = RandomUtil.generateRandomLongValue(1, 50);
            requestsPerStatKey.put(statKey, requests);
            for (int i = 0; i < requests; i++) {
                statisticManager.addStatistic(requestType, 0, 0, statKey);
            }
        }

        return requestsPerStatKey;
    }

    public HashMap<StatisticKey, Long> addCumulativeMessageSize(TCPRequestType requestType, boolean isRead) {

        HashMap<StatisticKey, Long> messageSizePerStatKey = new HashMap<>();
        long sizePerMessage = 20;

        for (StatisticKey statKey : statisticKeys) {
            long messages = RandomUtil.generateRandomLongValue(1, 50);
            messageSizePerStatKey.put(statKey, messages * sizePerMessage);
            if (isRead) {
                for (int i = 0; i < messages; i++) {
                    statisticManager.addStatistic(requestType, sizePerMessage, 0, statKey);
                }
            } else {
                for (int i = 0; i < messages; i++) {
                    statisticManager.addStatistic(requestType, 0, sizePerMessage, statKey);
                }
            }
        }

        return messageSizePerStatKey;
    }

    public HashMap<StatisticKey, Long> addCumulativeMessageSize2(TCPRequestType requestType, boolean isRead) {

        HashMap<StatisticKey, Long> messageSizePerStatKey = new HashMap<>();
        long sizePerMessage = 20;

        for (StatisticKey statKey : statisticKeys) {
            long messages = RandomUtil.generateRandomLongValue(1, 50);
            messageSizePerStatKey.put(statKey, messages * sizePerMessage);
            if (isRead) {
                for (int i = 0; i < messages; i++) {
                    statisticManager.addStatistic(requestType, sizePerMessage, 0, statKey.getKey(), statKey.getClientZone());
                }
            } else {
                for (int i = 0; i < messages; i++) {
                    statisticManager.addStatistic(requestType, 0, sizePerMessage, statKey.getKey(), statKey.getClientZone());
                }
            }
        }

        return messageSizePerStatKey;
    }

}
