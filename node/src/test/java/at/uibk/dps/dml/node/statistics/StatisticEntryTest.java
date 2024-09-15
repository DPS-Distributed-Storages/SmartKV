package at.uibk.dps.dml.node.statistics;

import at.uibk.dps.dml.node.billing.TCPRequestType;
import io.netty.util.internal.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.EnumMap;

import static org.junit.jupiter.api.Assertions.*;

public class StatisticEntryTest {

    private StatisticEntry statisticEntry;

    @BeforeEach
    public void init() {
        statisticEntry = new StatisticEntry();
    }

    @Test
    public void testInitialization() {
        EnumMap<TCPRequestType, Long> numberOfRequests = statisticEntry.getNumberOfRequests();
        EnumMap<TCPRequestType, Long> cumulativeMessageSizeReceived = statisticEntry.getCumulativeMessageSizeReceived();
        EnumMap<TCPRequestType, Long> cumulativeMessageSizeSent = statisticEntry.getCumulativeMessageSizeSent();
        assertEquals(TCPRequestType.values().length, numberOfRequests.size());
        assertEquals(TCPRequestType.values().length, cumulativeMessageSizeReceived.size());
        assertEquals(TCPRequestType.values().length, cumulativeMessageSizeSent.size());
        for (TCPRequestType type : TCPRequestType.values()) {
            assertEquals(0L, numberOfRequests.get(type));
            assertEquals(0L, cumulativeMessageSizeReceived.get(type));
            assertEquals(0L, cumulativeMessageSizeSent.get(type));
        }
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
    public void testAddCumulativeMessageSizeReceivedPUT() {
        testAddCumulativeMessageSizeReceived(TCPRequestType.PUT);
    }

    @Test
    public void testAddCumulativeMessageSizeReceivedGET() {
        testAddCumulativeMessageSizeReceived(TCPRequestType.GET);
    }

    @Test
    public void testAddCumulativeMessageSizeSentPUT() {
        testAddCumulativeMessageSizeSent(TCPRequestType.PUT);
    }

    @Test
    public void testAddCumulativeMessageSizeSentGET() {
        testAddCumulativeMessageSizeSent(TCPRequestType.GET);
    }

    @Test
    public void testResetToZero() {
        addRequests(TCPRequestType.PUT);
        addRequests(TCPRequestType.GET);
        addCumulativeMessageSize(TCPRequestType.PUT, true);
        addCumulativeMessageSize(TCPRequestType.GET, true);
        addCumulativeMessageSize(TCPRequestType.PUT, false);
        addCumulativeMessageSize(TCPRequestType.GET, false);

        Arrays.stream(TCPRequestType.values()).forEach(type -> {
            assertNotEquals(0, statisticEntry.getNumberOfRequests().get(type));
            assertNotEquals(0, statisticEntry.getCumulativeMessageSizeReceived().get(type));
            assertNotEquals(0, statisticEntry.getCumulativeMessageSizeSent().get(type));
        });

        statisticEntry.resetToZero();

        Arrays.stream(TCPRequestType.values()).forEach(type -> {
            assertEquals(0, statisticEntry.getNumberOfRequests().get(type));
            assertEquals(0, statisticEntry.getCumulativeMessageSizeReceived().get(type));
            assertEquals(0, statisticEntry.getCumulativeMessageSizeSent().get(type));
        });

    }

    protected static long generateRandomLongValue(long min, long max){
        return ThreadLocalRandom.current().nextLong(min, max);
    }

    private void testAddRequests(TCPRequestType requestType){
        long requestCounts = addRequests(requestType);
        assertEquals(requestCounts, statisticEntry.getNumberOfRequests().get(requestType));
        Arrays.stream(TCPRequestType.values())
                .filter(type -> type != requestType)
                .forEach( request ->
                        assertEquals(0L, statisticEntry.getNumberOfRequests().get(request))
                );
    }

    private long addRequests(TCPRequestType requestType){
        long requestCounts = generateRandomLongValue(10, 50);
        for(int i = 0; i < requestCounts; i++){
            statisticEntry.addRequest(requestType);
        }
        return requestCounts;
    }

    private void testAddCumulativeMessageSizeReceived(TCPRequestType requestType){
        testAddCumulativeMessageSize(requestType, true);
    }

    private void testAddCumulativeMessageSizeSent(TCPRequestType requestType){
        testAddCumulativeMessageSize(requestType, false);
    }

    private void testAddCumulativeMessageSize(TCPRequestType requestType, boolean isReceived){

        long cumulativeMessageSize = addCumulativeMessageSize(requestType, isReceived);
        if(isReceived) {
            assertEquals(cumulativeMessageSize, statisticEntry.getCumulativeMessageSizeReceived().get(requestType));
        }else{
            assertEquals(cumulativeMessageSize, statisticEntry.getCumulativeMessageSizeSent().get(requestType));
        }
        Arrays.stream(TCPRequestType.values())
                .filter(type -> type != requestType)
                .forEach( request -> {
                            assertEquals(0L, statisticEntry.getCumulativeMessageSizeReceived().get(request));
                            assertEquals(0L, statisticEntry.getCumulativeMessageSizeSent().get(request));
                        }
                );
    }

    private long addCumulativeMessageSize(TCPRequestType requestType, boolean isReceived){
        long requestCounts = generateRandomLongValue(10, 50);
        long cumulativeMessageSizeReceived = 0;
        if(isReceived) {
            for (int i = 0; i < requestCounts; i++) {
                long messageSize = generateRandomLongValue(200, 500);
                statisticEntry.addReceivedMessageSize(requestType, messageSize);
                cumulativeMessageSizeReceived += messageSize;
            }
        }else{
            for (int i = 0; i < requestCounts; i++) {
                long messageSize = generateRandomLongValue(200, 500);
                statisticEntry.addSentMessageSize(requestType, messageSize);
                cumulativeMessageSizeReceived += messageSize;
            }
        }
        return cumulativeMessageSizeReceived;
    }

}
