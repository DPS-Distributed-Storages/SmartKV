package at.uibk.dps.dml.node.billing;

import at.uibk.dps.dml.node.membership.DataTransferPricingCategory;
import at.uibk.dps.dml.node.util.RandomUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.EnumMap;

import static org.junit.jupiter.api.Assertions.*;

public class TCPCommunicationMetricsTest {

    private TCPCommunicationMetrics tcpCommunicationMetrics;

    @BeforeEach
    public void init() {
        tcpCommunicationMetrics = new TCPCommunicationMetrics();
    }

    @Test
    public void testInitialization() {
        EnumMap numberOfRequests = tcpCommunicationMetrics.getNumberOfRequests();
        assertEquals(TCPRequestType.values().length, numberOfRequests.size());
        for (TCPRequestType type : TCPRequestType.values()) {
            assertEquals(0L, numberOfRequests.get(type));
        }

        EnumMap<DataTransferPricingCategory, Long> dataTransferOut = tcpCommunicationMetrics.getDataTransferOut();
        EnumMap<DataTransferPricingCategory, Long> dataTransferIn = tcpCommunicationMetrics.getDataTransferIn();

        for (DataTransferPricingCategory category : DataTransferPricingCategory.values()) {
            assertEquals(Long.valueOf(0), dataTransferOut.get(category));
            assertEquals(Long.valueOf(0), dataTransferIn.get(category));
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
    public void testAddPutGetRequests() {
        testAddRequests(TCPRequestType.GET);
    }

    @Test
    public void testAddPUTRequest() {
        testAddRequest(TCPRequestType.PUT);
    }

    @Test
    public void testAddGETRequest() {
        testAddRequest(TCPRequestType.GET);
    }

    @Test
    public void testAddWrongRequest() {
        assertThrows(NullPointerException.class, () -> tcpCommunicationMetrics.addRequest(RPCRequestType.GET));
        assertThrows(NullPointerException.class, () -> tcpCommunicationMetrics.addRequest(RPCRequestType.INVALIDATE));
        assertThrows(NullPointerException.class, () -> tcpCommunicationMetrics.addRequest(RPCRequestType.COMMIT));
        assertEquals(0L, tcpCommunicationMetrics.getNumberOfRequests().get(TCPRequestType.PUT));
        assertEquals(0L, tcpCommunicationMetrics.getNumberOfRequests().get(TCPRequestType.GET));
    }

    @Test
    public void testSetNumberOfRequests() {
        long numberOfPUTRequests = RandomUtil.generateRandomLongValue();
        long numberOfGETRequests = RandomUtil.generateRandomLongValue();
        EnumMap<TCPRequestType, Long> numberOfRequests = new EnumMap<>(TCPRequestType.class);
        numberOfRequests.put(TCPRequestType.PUT, numberOfPUTRequests);
        numberOfRequests.put(TCPRequestType.GET, numberOfGETRequests);

        tcpCommunicationMetrics.setNumberOfRequests(numberOfRequests);

        assertEquals(numberOfPUTRequests, tcpCommunicationMetrics.getNumberOfRequests().get(TCPRequestType.PUT));
        assertEquals(numberOfGETRequests, tcpCommunicationMetrics.getNumberOfRequests().get(TCPRequestType.GET));
    }

    @Test
    public void testAddDataTransferOutSameProvider() {
        testAddDataTransferOut(DataTransferPricingCategory.SAME_PROVIDER);
    }

    @Test
    public void testAddDataTransferOutSameRegion() {
        testAddDataTransferOut(DataTransferPricingCategory.SAME_REGION);
    }

    @Test
    public void testAddDataTransferOutInternet() {
        testAddDataTransferOut(DataTransferPricingCategory.INTERNET);
    }

    @Test
    public void testAddDataTransferInSameProvider() {
        testAddDataTransferIn(DataTransferPricingCategory.SAME_PROVIDER);
    }

    @Test
    public void testAddDataTransferInSameRegion() {
        testAddDataTransferIn(DataTransferPricingCategory.SAME_REGION);
    }

    @Test
    public void testAddDataTransferInInternet() {
        testAddDataTransferIn(DataTransferPricingCategory.INTERNET);
    }

    @Test
    public void testResetToZero() {
        addRequests(TCPRequestType.PUT);
        addRequests(TCPRequestType.GET);
        addDataTransferIn(DataTransferPricingCategory.SAME_PROVIDER);
        addDataTransferIn(DataTransferPricingCategory.SAME_REGION);
        addDataTransferIn(DataTransferPricingCategory.INTERNET);
        addDataTransferOut(DataTransferPricingCategory.SAME_PROVIDER);
        addDataTransferOut(DataTransferPricingCategory.SAME_REGION);
        addDataTransferOut(DataTransferPricingCategory.INTERNET);
        assertFalse(tcpCommunicationsAreReset());
        tcpCommunicationMetrics.resetToZero();
        assertTrue(tcpCommunicationsAreReset());
    }

    private boolean tcpCommunicationsAreReset(){
        return CommunicationUtil.communicationsAreReset(this.tcpCommunicationMetrics);
    }

    private void testAddRequests(TCPRequestType requestType){
        long requestCounts = addRequests(requestType);
        assertEquals(requestCounts, tcpCommunicationMetrics.getNumberOfRequests().get(requestType));
        Arrays.stream(TCPRequestType.values())
                .filter(type -> type != requestType)
                .forEach( request ->
                        assertEquals(0L, tcpCommunicationMetrics.getNumberOfRequests().get(request))
                );
    }

    private long addRequests(TCPRequestType requestType) {
        long[] requestCounts = RandomUtil.generateRandomLongValues();
        for (long requestCount : requestCounts)
            tcpCommunicationMetrics.addRequests(requestType, requestCount);
        return RandomUtil.sumLongValues(requestCounts);
    }


    private void testAddRequest(TCPRequestType requestType){
        long numberOfRequests = addRequest(requestType);
        assertEquals(numberOfRequests, tcpCommunicationMetrics.getNumberOfRequests().get(requestType));
        Arrays.stream(TCPRequestType.values())
                .filter(type -> type != requestType)
                .forEach( request ->
                        assertEquals(0L, tcpCommunicationMetrics.getNumberOfRequests().get(request))
                );
    }

    private long addRequest(TCPRequestType requestType){
        long numberOfRequests = RandomUtil.generateRandomLongValue();
        for(int i = 0; i < numberOfRequests; i++)
            tcpCommunicationMetrics.addRequest(requestType);
        return numberOfRequests;
    }

    private void testAddDataTransferOut(DataTransferPricingCategory dataTransferPricingCategory) {
        long addedTransfer = addDataTransferOut(dataTransferPricingCategory);
        assertEquals(addedTransfer, tcpCommunicationMetrics.getDataTransferOut().get(dataTransferPricingCategory));
        Arrays.stream(DataTransferPricingCategory.values())
                .filter(type -> type != dataTransferPricingCategory)
                .forEach( category ->
                        assertEquals(0L, tcpCommunicationMetrics.getDataTransferOut().get(category))
                );
    }

    private long addDataTransferOut(DataTransferPricingCategory dataTransferPricingCategory) {
        long[] randomSizes = RandomUtil.generateRandomLongValues();
        for(long randomSize : randomSizes)
            tcpCommunicationMetrics.addDataTransferOut(dataTransferPricingCategory, randomSize);
        return RandomUtil.sumLongValues(randomSizes);
    }

    private void testAddDataTransferIn(DataTransferPricingCategory dataTransferPricingCategory) {
        long addedTransfer = addDataTransferIn(dataTransferPricingCategory);
        assertEquals(addedTransfer, tcpCommunicationMetrics.getDataTransferIn().get(dataTransferPricingCategory));
        Arrays.stream(DataTransferPricingCategory.values())
                .filter(type -> type != dataTransferPricingCategory)
                .forEach( category ->
                        assertEquals(0L, tcpCommunicationMetrics.getDataTransferIn().get(category))
                );
    }

    private long addDataTransferIn(DataTransferPricingCategory dataTransferPricingCategory) {
        long[] randomSizes = RandomUtil.generateRandomLongValues();
        for(long randomSize : randomSizes)
            tcpCommunicationMetrics.addDataTransferIn(dataTransferPricingCategory, randomSize);
        return RandomUtil.sumLongValues(randomSizes);
    }

}
