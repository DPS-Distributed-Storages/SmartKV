package at.uibk.dps.dml.node.billing;

import at.uibk.dps.dml.node.membership.DataTransferPricingCategory;
import at.uibk.dps.dml.node.util.RandomUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.EnumMap;

import static org.junit.jupiter.api.Assertions.*;

public class RPCCommunicationMetricsTest {

    private RPCCommunicationMetrics rpcCommunicationMetrics;

    @BeforeEach
    public void init() {
        rpcCommunicationMetrics = new RPCCommunicationMetrics();
    }

    @Test
    public void testInitialization() {
        EnumMap numberOfRequests = rpcCommunicationMetrics.getNumberOfRequests();
        assertEquals(RPCRequestType.values().length, numberOfRequests.size());
        for (RPCRequestType type : RPCRequestType.values()) {
            assertEquals(0L, numberOfRequests.get(type));
        }

        EnumMap<DataTransferPricingCategory, Long> dataTransferOut = rpcCommunicationMetrics.getDataTransferOut();
        EnumMap<DataTransferPricingCategory, Long> dataTransferIn = rpcCommunicationMetrics.getDataTransferIn();

        for (DataTransferPricingCategory category : DataTransferPricingCategory.values()) {
            assertEquals(Long.valueOf(0), dataTransferOut.get(category));
            assertEquals(Long.valueOf(0), dataTransferIn.get(category));
        }
    }

    @Test
    public void testAddInvalidateRequests() {
        testAddRequests(RPCRequestType.INVALIDATE);
    }

    @Test
    public void testAddGetRequests() {
        testAddRequests(RPCRequestType.GET);
    }

    @Test
    public void testAddCommitRequests() {
        testAddRequests(RPCRequestType.COMMIT);
    }

    @Test
    public void testAddInvalidateGetCommitRequests() {
        long[] requestCountsInvalidate = RandomUtil.generateRandomLongValues(15);
        long[] requestCountsGET = RandomUtil.generateRandomLongValues(20);
        long[] requestCountsCommit = RandomUtil.generateRandomLongValues(10);
        for(long requestCount : requestCountsInvalidate)
            rpcCommunicationMetrics.addRequests(RPCRequestType.INVALIDATE, requestCount);
        for(long requestCount : requestCountsGET)
            rpcCommunicationMetrics.addRequests(RPCRequestType.GET, requestCount);
        for(long requestCount : requestCountsCommit)
            rpcCommunicationMetrics.addRequests(RPCRequestType.COMMIT, requestCount);
        assertEquals(RandomUtil.sumLongValues(requestCountsInvalidate), rpcCommunicationMetrics.getNumberOfRequests().get(RPCRequestType.INVALIDATE));
        assertEquals(RandomUtil.sumLongValues(requestCountsGET), rpcCommunicationMetrics.getNumberOfRequests().get(RPCRequestType.GET));
        assertEquals(RandomUtil.sumLongValues(requestCountsCommit), rpcCommunicationMetrics.getNumberOfRequests().get(RPCRequestType.COMMIT));
    }

    @Test
    public void testAddInvalidateRequest() {
        testAddRequest(RPCRequestType.INVALIDATE);
    }

    @Test
    public void testAddGETRequest() {
        testAddRequest(RPCRequestType.GET);
    }

    @Test
    public void testAddCommitRequest() {
        testAddRequest(RPCRequestType.COMMIT);
    }


    @Test
    public void testAddWrongRequest() {
        assertThrows(NullPointerException.class, () -> rpcCommunicationMetrics.addRequest(TCPRequestType.PUT));
        assertThrows(NullPointerException.class, () -> rpcCommunicationMetrics.addRequest(TCPRequestType.GET));
        assertEquals(0L, rpcCommunicationMetrics.getNumberOfRequests().get(RPCRequestType.INVALIDATE));
        assertEquals(0L, rpcCommunicationMetrics.getNumberOfRequests().get(RPCRequestType.GET));
        assertEquals(0L, rpcCommunicationMetrics.getNumberOfRequests().get(RPCRequestType.COMMIT));
    }

    @Test
    public void testSetNumberOfRequests() {
        long numberOfInvalidateRequests = RandomUtil.generateRandomLongValue();
        long numberOfGETRequests = RandomUtil.generateRandomLongValue();
        long numberOfCommitRequests = RandomUtil.generateRandomLongValue();
        EnumMap<RPCRequestType, Long> numberOfRequests = new EnumMap<>(RPCRequestType.class);
        numberOfRequests.put(RPCRequestType.INVALIDATE, numberOfInvalidateRequests);
        numberOfRequests.put(RPCRequestType.GET, numberOfGETRequests);
        numberOfRequests.put(RPCRequestType.COMMIT, numberOfCommitRequests);

        rpcCommunicationMetrics.setNumberOfRequests(numberOfRequests);

        assertEquals(numberOfInvalidateRequests, rpcCommunicationMetrics.getNumberOfRequests().get(RPCRequestType.INVALIDATE));
        assertEquals(numberOfGETRequests, rpcCommunicationMetrics.getNumberOfRequests().get(RPCRequestType.GET));
        assertEquals(numberOfCommitRequests, rpcCommunicationMetrics.getNumberOfRequests().get(RPCRequestType.COMMIT));
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
        addRequests(RPCRequestType.INVALIDATE);
        addRequests(RPCRequestType.GET);
        addRequests(RPCRequestType.COMMIT);
        addDataTransferIn(DataTransferPricingCategory.SAME_PROVIDER);
        addDataTransferIn(DataTransferPricingCategory.SAME_REGION);
        addDataTransferIn(DataTransferPricingCategory.INTERNET);
        addDataTransferOut(DataTransferPricingCategory.SAME_PROVIDER);
        addDataTransferOut(DataTransferPricingCategory.SAME_REGION);
        addDataTransferOut(DataTransferPricingCategory.INTERNET);
        assertFalse(rpcCommunicationsAreReset());
        rpcCommunicationMetrics.resetToZero();
        assertTrue(rpcCommunicationsAreReset());
    }

    private boolean rpcCommunicationsAreReset(){
        return CommunicationUtil.communicationsAreReset(this.rpcCommunicationMetrics);
    }

    private void testAddRequests(RPCRequestType requestType){
        long requestCounts = addRequests(requestType);
        assertEquals(requestCounts, rpcCommunicationMetrics.getNumberOfRequests().get(requestType));
        Arrays.stream(RPCRequestType.values())
                .filter(type -> type != requestType)
                .forEach( request ->
                        assertEquals(0L, rpcCommunicationMetrics.getNumberOfRequests().get(request))
                );
    }

    private long addRequests(RPCRequestType requestType) {
        long[] requestCounts = RandomUtil.generateRandomLongValues();
        for (long requestCount : requestCounts)
            rpcCommunicationMetrics.addRequests(requestType, requestCount);
        return RandomUtil.sumLongValues(requestCounts);
    }

    private void testAddRequest(RPCRequestType requestType){
        long numberOfRequests = addRequest(requestType);
        assertEquals(numberOfRequests, rpcCommunicationMetrics.getNumberOfRequests().get(requestType));
        Arrays.stream(RPCRequestType.values())
                .filter(type -> type != requestType)
                .forEach( request ->
                        assertEquals(0L, rpcCommunicationMetrics.getNumberOfRequests().get(request))
                );
    }

    private long addRequest(RPCRequestType requestType){
        long numberOfRequests = RandomUtil.generateRandomLongValue();
        for(int i = 0; i < numberOfRequests; i++)
            rpcCommunicationMetrics.addRequest(requestType);
        return numberOfRequests;
    }

    private void testAddDataTransferOut(DataTransferPricingCategory dataTransferPricingCategory) {
        long addedTransfer = addDataTransferOut(dataTransferPricingCategory);
        assertEquals(addedTransfer, rpcCommunicationMetrics.getDataTransferOut().get(dataTransferPricingCategory));
        Arrays.stream(DataTransferPricingCategory.values())
                .filter(type -> type != dataTransferPricingCategory)
                .forEach( category ->
                        assertEquals(0L, rpcCommunicationMetrics.getDataTransferOut().get(category))
                );
    }

    private long addDataTransferOut(DataTransferPricingCategory dataTransferPricingCategory) {
        long[] randomSizes = RandomUtil.generateRandomLongValues();
        for(long randomSize : randomSizes)
            rpcCommunicationMetrics.addDataTransferOut(dataTransferPricingCategory, randomSize);
        return RandomUtil.sumLongValues(randomSizes);
    }

        private void testAddDataTransferIn(DataTransferPricingCategory dataTransferPricingCategory) {
        long addedTransfer = addDataTransferIn(dataTransferPricingCategory);
        assertEquals(addedTransfer, rpcCommunicationMetrics.getDataTransferIn().get(dataTransferPricingCategory));
        Arrays.stream(DataTransferPricingCategory.values())
                .filter(type -> type != dataTransferPricingCategory)
                .forEach( category ->
                        assertEquals(0L, rpcCommunicationMetrics.getDataTransferIn().get(category))
                );
    }

    private long addDataTransferIn(DataTransferPricingCategory dataTransferPricingCategory) {
        long[] randomSizes = RandomUtil.generateRandomLongValues();
        for(long randomSize : randomSizes)
            rpcCommunicationMetrics.addDataTransferIn(dataTransferPricingCategory, randomSize);
        return RandomUtil.sumLongValues(randomSizes);
    }

}
