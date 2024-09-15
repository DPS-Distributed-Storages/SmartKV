package at.uibk.dps.dml.node.billing;

import at.uibk.dps.dml.node.membership.*;
import at.uibk.dps.dml.node.util.MemoryUtil;
import at.uibk.dps.dml.node.util.NodeLocation;
import at.uibk.dps.dml.node.util.RandomUtil;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class BillingServiceTest {
    @Mock
    private MembershipManager membershipManager;

    private MembershipView membershipView;

    private BillingService billingService;

    private static int VERTICLE_SAME_REGION_ID = 9001;

    private static int VERTICLE_SAME_PROVIDER_ID = 9002;

    private static int VERTICLE_DIFFERENT_ID = 9003;

    private static String SAME_REGION = "localRegion";

    private static String SAME_PROVIDER = "localProvider";

    private static String DIFFERENT_REGION = "region1";

    private static String DIFFERENT_PROVIDER = "provider1";

    @BeforeEach
    void init(Vertx vertx, VertxTestContext vertxTestContext) {
        Map<String, DmlNodeInfo> nodeMap = new HashMap<>();
        DmlNodeInfo nodeInfo1 = new DmlNodeInfo(SAME_REGION, "", SAME_PROVIDER, "host1", 1, false, new DmlNodeUnitPrices(), null);
        DmlNodeInfo nodeInfo2 = new DmlNodeInfo(DIFFERENT_REGION, "", SAME_PROVIDER, "host2", 1, false, new DmlNodeUnitPrices(), null);
        DmlNodeInfo nodeInfo3 = new DmlNodeInfo(DIFFERENT_REGION, "", DIFFERENT_PROVIDER, "host3", 1, false, new DmlNodeUnitPrices(), null);

        Set<VerticleInfo> verticles_region_1 = new HashSet<>();
        verticles_region_1.add(new VerticleInfo(VERTICLE_SAME_REGION_ID, VerticleType.STORAGE, VERTICLE_SAME_REGION_ID, nodeInfo1));
        nodeInfo1.setVerticles(verticles_region_1);

        Set<VerticleInfo> verticles_region_2 = new HashSet<>();
        verticles_region_2.add(new VerticleInfo(VERTICLE_SAME_PROVIDER_ID, VerticleType.STORAGE, VERTICLE_SAME_PROVIDER_ID, nodeInfo2));
        nodeInfo2.setVerticles(verticles_region_2);

        Set<VerticleInfo> verticles_region_3 = new HashSet<>();
        verticles_region_3.add(new VerticleInfo(VERTICLE_DIFFERENT_ID, VerticleType.STORAGE, VERTICLE_DIFFERENT_ID, nodeInfo3));
        nodeInfo3.setVerticles(verticles_region_3);

        nodeMap.put("host1", nodeInfo1);
        nodeMap.put("host2", nodeInfo2);
        nodeMap.put("host3", nodeInfo3);
        membershipView = new MembershipView(0, nodeMap);

        MockitoAnnotations.openMocks(this);
        when(membershipManager.getMembershipView()).thenReturn(membershipView);

        // For testing purposes, we set the tracking time and billing time very high (1 Day)
        int aDayInMillis = 1000 * 60 * 60 * 24;

        BackendRegistries.setupBackend(new MicrometerMetricsOptions());

        billingService = new BillingService(vertx, membershipManager, SAME_REGION, SAME_PROVIDER, 0, new DmlNodeUnitPrices(), aDayInMillis, aDayInMillis);
        vertxTestContext.completeNow();
    }

    @Test
    void testAddTcpDataTransferIn(Vertx vertx, VertxTestContext vertxTestContext) {
        long dataTransferInBytesSameRegion = RandomUtil.generateRandomLongValue();
        billingService.addTcpDataTransferIn(dataTransferInBytesSameRegion, SAME_REGION, SAME_PROVIDER);

        long dataTransferInBytesSameProvider = dataTransferInBytesSameRegion + RandomUtil.generateRandomLongValue();
        billingService.addTcpDataTransferIn(dataTransferInBytesSameProvider, DIFFERENT_REGION, SAME_PROVIDER);

        long dataTransferInBytesInternet = dataTransferInBytesSameProvider + RandomUtil.generateRandomLongValue();
        billingService.addTcpDataTransferIn(dataTransferInBytesInternet, DIFFERENT_REGION, DIFFERENT_PROVIDER);

        long dataTransferInBytesInternet2 = dataTransferInBytesInternet + RandomUtil.generateRandomLongValue();
        billingService.addTcpDataTransferIn(dataTransferInBytesInternet2, SAME_REGION, DIFFERENT_PROVIDER);

        waitForPendingEvents(vertx).onComplete( res -> {
            vertxTestContext.verify(() -> {
                assertEquals(dataTransferInBytesSameRegion, billingService.getTcpCommunicationMetrics().getDataTransferIn().get(DataTransferPricingCategory.SAME_REGION));
                assertEquals(dataTransferInBytesSameProvider, billingService.getTcpCommunicationMetrics().getDataTransferIn().get(DataTransferPricingCategory.SAME_PROVIDER));
                assertEquals(dataTransferInBytesInternet + dataTransferInBytesInternet2, billingService.getTcpCommunicationMetrics().getDataTransferIn().get(DataTransferPricingCategory.INTERNET));
                assertEquals(0L, billingService.getTcpCommunicationMetrics().getDataTransferOut().values().stream().mapToLong(Long::longValue).sum());
                assertEquals(0L, billingService.getRpcCommunicationMetrics().getDataTransferIn().values().stream().mapToLong(Long::longValue).sum());
                assertEquals(0L, billingService.getRpcCommunicationMetrics().getDataTransferOut().values().stream().mapToLong(Long::longValue).sum());
                vertxTestContext.completeNow();
            });
        });
    }

    @Test
    void testAddTcpDataTransferInByNodeLocation(Vertx vertx, VertxTestContext vertxTestContext) {
        long dataTransferInBytesSameRegion = RandomUtil.generateRandomLongValue();
        billingService.addTcpDataTransferIn(dataTransferInBytesSameRegion, new NodeLocation(SAME_REGION, "", SAME_PROVIDER));

        long dataTransferInBytesSameProvider = dataTransferInBytesSameRegion + RandomUtil.generateRandomLongValue();
        billingService.addTcpDataTransferIn(dataTransferInBytesSameProvider, new NodeLocation(DIFFERENT_REGION, "", SAME_PROVIDER));

        long dataTransferInBytesInternet = dataTransferInBytesSameProvider + RandomUtil.generateRandomLongValue();
        billingService.addTcpDataTransferIn(dataTransferInBytesInternet, new NodeLocation(DIFFERENT_REGION, "", DIFFERENT_PROVIDER));

        long dataTransferInBytesInternet2 = dataTransferInBytesInternet + RandomUtil.generateRandomLongValue();
        billingService.addTcpDataTransferIn(dataTransferInBytesInternet2, new NodeLocation(SAME_REGION, "", DIFFERENT_PROVIDER));

        waitForPendingEvents(vertx).onComplete( res -> {
            vertxTestContext.verify(() -> {
                assertEquals(dataTransferInBytesSameRegion, billingService.getTcpCommunicationMetrics().getDataTransferIn().get(DataTransferPricingCategory.SAME_REGION));
                assertEquals(dataTransferInBytesSameProvider, billingService.getTcpCommunicationMetrics().getDataTransferIn().get(DataTransferPricingCategory.SAME_PROVIDER));
                assertEquals(dataTransferInBytesInternet + dataTransferInBytesInternet2, billingService.getTcpCommunicationMetrics().getDataTransferIn().get(DataTransferPricingCategory.INTERNET));
                vertxTestContext.completeNow();
            });
        });
    }

    @Test
    void testAddTcpDataTransferOut(Vertx vertx, VertxTestContext vertxTestContext) {
        long dataTransferOutBytesSameRegion = RandomUtil.generateRandomLongValue();
        billingService.addTcpDataTransferOut(dataTransferOutBytesSameRegion, SAME_REGION, SAME_PROVIDER);

        long dataTransferOutBytesSameProvider = dataTransferOutBytesSameRegion + RandomUtil.generateRandomLongValue();
        billingService.addTcpDataTransferOut(dataTransferOutBytesSameProvider, DIFFERENT_REGION, SAME_PROVIDER);

        long dataTransferOutBytesInternet = dataTransferOutBytesSameProvider + RandomUtil.generateRandomLongValue();
        billingService.addTcpDataTransferOut(dataTransferOutBytesInternet, DIFFERENT_REGION, DIFFERENT_PROVIDER);

        long dataTransferOutBytesInternet2 = dataTransferOutBytesInternet + RandomUtil.generateRandomLongValue();
        billingService.addTcpDataTransferOut(dataTransferOutBytesInternet2, SAME_REGION, DIFFERENT_PROVIDER);

        waitForPendingEvents(vertx).onComplete( res -> {
            vertxTestContext.verify(() -> {
                assertEquals(dataTransferOutBytesSameRegion, billingService.getTcpCommunicationMetrics().getDataTransferOut().get(DataTransferPricingCategory.SAME_REGION));
                assertEquals(dataTransferOutBytesSameProvider, billingService.getTcpCommunicationMetrics().getDataTransferOut().get(DataTransferPricingCategory.SAME_PROVIDER));
                assertEquals(dataTransferOutBytesInternet + dataTransferOutBytesInternet2, billingService.getTcpCommunicationMetrics().getDataTransferOut().get(DataTransferPricingCategory.INTERNET));
                vertxTestContext.completeNow();
            });
        });
    }

    @Test
    void testAddTcpDataTransferOutByNodeLocation(Vertx vertx, VertxTestContext vertxTestContext) {
        long dataTransferOutBytesSameRegion = RandomUtil.generateRandomLongValue();
        billingService.addTcpDataTransferOut(dataTransferOutBytesSameRegion, new NodeLocation(SAME_REGION, "", SAME_PROVIDER));

        long dataTransferOutBytesSameProvider = dataTransferOutBytesSameRegion + RandomUtil.generateRandomLongValue();
        billingService.addTcpDataTransferOut(dataTransferOutBytesSameProvider, new NodeLocation(DIFFERENT_REGION, "", SAME_PROVIDER));

        long dataTransferOutBytesInternet = dataTransferOutBytesSameProvider + RandomUtil.generateRandomLongValue();
        billingService.addTcpDataTransferOut(dataTransferOutBytesInternet, new NodeLocation(DIFFERENT_REGION, "", DIFFERENT_PROVIDER));

        long dataTransferOutBytesInternet2 = dataTransferOutBytesInternet + RandomUtil.generateRandomLongValue();
        billingService.addTcpDataTransferOut(dataTransferOutBytesInternet2, new NodeLocation(SAME_REGION, "", DIFFERENT_PROVIDER));

        waitForPendingEvents(vertx).onComplete( res -> {
            vertxTestContext.verify(() -> {
                assertEquals(dataTransferOutBytesSameRegion, billingService.getTcpCommunicationMetrics().getDataTransferOut().get(DataTransferPricingCategory.SAME_REGION));
                assertEquals(dataTransferOutBytesSameProvider, billingService.getTcpCommunicationMetrics().getDataTransferOut().get(DataTransferPricingCategory.SAME_PROVIDER));
                assertEquals(dataTransferOutBytesInternet + dataTransferOutBytesInternet2, billingService.getTcpCommunicationMetrics().getDataTransferOut().get(DataTransferPricingCategory.INTERNET));
                vertxTestContext.completeNow();
            });
        });
    }

    @Test
    void testAddRpcDataTransferIn(Vertx vertx, VertxTestContext vertxTestContext) {

        long dataTransferInBytesSameRegion = 24;
        billingService.addRpcDataTransferIn(dataTransferInBytesSameRegion, SAME_REGION, SAME_PROVIDER);

        long dataTransferInBytesSameProvider = dataTransferInBytesSameRegion + RandomUtil.generateRandomLongValue();
        billingService.addRpcDataTransferIn(dataTransferInBytesSameProvider, DIFFERENT_REGION, SAME_PROVIDER);

        long dataTransferInBytesInternet = dataTransferInBytesSameProvider + RandomUtil.generateRandomLongValue();
        billingService.addRpcDataTransferIn(dataTransferInBytesInternet, DIFFERENT_REGION, DIFFERENT_PROVIDER);

        long dataTransferInBytesInternet2 = dataTransferInBytesInternet + RandomUtil.generateRandomLongValue();
        billingService.addRpcDataTransferIn(dataTransferInBytesInternet2, SAME_REGION, DIFFERENT_PROVIDER);

        waitForPendingEvents(vertx).onComplete( res -> {
            vertxTestContext.verify(() -> {
                assertEquals(dataTransferInBytesSameRegion, billingService.getRpcCommunicationMetrics().getDataTransferIn().get(DataTransferPricingCategory.SAME_REGION));
                assertEquals(dataTransferInBytesSameProvider, billingService.getRpcCommunicationMetrics().getDataTransferIn().get(DataTransferPricingCategory.SAME_PROVIDER));
                assertEquals(dataTransferInBytesInternet + dataTransferInBytesInternet2, billingService.getRpcCommunicationMetrics().getDataTransferIn().get(DataTransferPricingCategory.INTERNET));
                vertxTestContext.completeNow();
            });
        });
    }

    @Test
    void testAddRpcDataTransferInByVerticleId(Vertx vertx, VertxTestContext vertxTestContext) throws InterruptedException {

        long dataTransferInBytesSameRegion = RandomUtil.generateRandomLongValue();
        billingService.addRpcDataTransferIn(dataTransferInBytesSameRegion, VERTICLE_SAME_REGION_ID);

        long dataTransferInBytesSameProvider = dataTransferInBytesSameRegion + RandomUtil.generateRandomLongValue();
        billingService.addRpcDataTransferIn(dataTransferInBytesSameProvider, VERTICLE_SAME_PROVIDER_ID);

        long dataTransferInBytesInternet = dataTransferInBytesSameProvider + RandomUtil.generateRandomLongValue();
        billingService.addRpcDataTransferIn(dataTransferInBytesInternet, VERTICLE_DIFFERENT_ID);

        waitForPendingEvents(vertx).onComplete( res -> {
            vertxTestContext.verify(() -> {
                assertEquals(dataTransferInBytesSameRegion, billingService.getRpcCommunicationMetrics().getDataTransferIn().get(DataTransferPricingCategory.SAME_REGION));
                assertEquals(dataTransferInBytesSameProvider, billingService.getRpcCommunicationMetrics().getDataTransferIn().get(DataTransferPricingCategory.SAME_PROVIDER));
                assertEquals(dataTransferInBytesInternet, billingService.getRpcCommunicationMetrics().getDataTransferIn().get(DataTransferPricingCategory.INTERNET));
                vertxTestContext.completeNow();
            });
        });
    }

    @Test
    void testAddRpcDataTransferOut(Vertx vertx, VertxTestContext vertxTestContext) {

        long dataTransferOutBytesSameRegion = 24;
        billingService.addRpcDataTransferOut(dataTransferOutBytesSameRegion, SAME_REGION, SAME_PROVIDER);

        long dataTransferOutBytesSameProvider = dataTransferOutBytesSameRegion + RandomUtil.generateRandomLongValue();
        billingService.addRpcDataTransferOut(dataTransferOutBytesSameProvider, DIFFERENT_REGION, SAME_PROVIDER);

        long dataTransferOutBytesInternet = dataTransferOutBytesSameProvider + RandomUtil.generateRandomLongValue();
        billingService.addRpcDataTransferOut(dataTransferOutBytesInternet, DIFFERENT_REGION, DIFFERENT_PROVIDER);

        long dataTransferOutBytesInternet2 = dataTransferOutBytesInternet + RandomUtil.generateRandomLongValue();
        billingService.addRpcDataTransferOut(dataTransferOutBytesInternet2, SAME_REGION, DIFFERENT_PROVIDER);

        waitForPendingEvents(vertx).onComplete( res -> {
            vertxTestContext.verify(() -> {
                assertEquals(dataTransferOutBytesSameRegion, billingService.getRpcCommunicationMetrics().getDataTransferOut().get(DataTransferPricingCategory.SAME_REGION));
                assertEquals(dataTransferOutBytesSameProvider, billingService.getRpcCommunicationMetrics().getDataTransferOut().get(DataTransferPricingCategory.SAME_PROVIDER));
                assertEquals(dataTransferOutBytesInternet + dataTransferOutBytesInternet2, billingService.getRpcCommunicationMetrics().getDataTransferOut().get(DataTransferPricingCategory.INTERNET));
                vertxTestContext.completeNow();
            });
        });
    }

    @Test
    void testAddRpcDataTransferOutByVerticleId(Vertx vertx, VertxTestContext vertxTestContext) {

        long dataTransferOutBytesSameRegion = RandomUtil.generateRandomLongValue();
        billingService.addRpcDataTransferOut(dataTransferOutBytesSameRegion, VERTICLE_SAME_REGION_ID);

        long dataTransferOutBytesSameProvider = dataTransferOutBytesSameRegion + RandomUtil.generateRandomLongValue();
        billingService.addRpcDataTransferOut(dataTransferOutBytesSameProvider, VERTICLE_SAME_PROVIDER_ID);

        long dataTransferOutBytesInternet = dataTransferOutBytesSameProvider + RandomUtil.generateRandomLongValue();
        billingService.addRpcDataTransferOut(dataTransferOutBytesInternet, VERTICLE_DIFFERENT_ID);

        waitForPendingEvents(vertx).onComplete( res -> {
            vertxTestContext.verify(() -> {
                assertEquals(dataTransferOutBytesSameRegion, billingService.getRpcCommunicationMetrics().getDataTransferOut().get(DataTransferPricingCategory.SAME_REGION));
                assertEquals(dataTransferOutBytesSameProvider, billingService.getRpcCommunicationMetrics().getDataTransferOut().get(DataTransferPricingCategory.SAME_PROVIDER));
                assertEquals(dataTransferOutBytesInternet, billingService.getRpcCommunicationMetrics().getDataTransferOut().get(DataTransferPricingCategory.INTERNET));
                vertxTestContext.completeNow();
            });
        });
    }

    @Test
    void testAddTcpPUTRequests(Vertx vertx, VertxTestContext vertxTestContext) {
        testAddTcpRequests(TCPRequestType.PUT, vertx, vertxTestContext);
    }

    @Test
    void testAddTcpGETRequests(Vertx vertx, VertxTestContext vertxTestContext) {
        testAddTcpRequests(TCPRequestType.GET, vertx, vertxTestContext);
    }

    @Test
    void testAddRpcInvalidateRequests(Vertx vertx, VertxTestContext vertxTestContext) {
        testAddRpcRequests(RPCRequestType.INVALIDATE, vertx, vertxTestContext);
    }

    @Test
    void testAddRpcGetRequests(Vertx vertx, VertxTestContext vertxTestContext) {
        testAddRpcRequests(RPCRequestType.GET, vertx, vertxTestContext);
    }

    @Test
    void testAddRpcCommitRequests(Vertx vertx, VertxTestContext vertxTestContext) {
        testAddRpcRequests(RPCRequestType.COMMIT, vertx, vertxTestContext);
    }

    @Test
    void testAddStorageBytes(Vertx vertx, VertxTestContext vertxTestContext){

        long addedBytes = 0;
        int runs = 10;

        for(int i = 0; i < runs; i++){
            byte[] buffer = new byte[RandomUtil.generateRandomIntegerValue(50,2048)];
            addedBytes += MemoryUtil.getDeepObjectSize(buffer);
            billingService.addStorageBytes(buffer);
        }

        long finalAddedBytes = addedBytes;
        waitForPendingEvents(vertx).onComplete(res -> {
            vertxTestContext.verify(() -> {
                Field trackedBytesField = billingService.getClass().getDeclaredField("totalStorageBytesCurrentTrackingGranularity");
                trackedBytesField.setAccessible(true);
                long trackedBytesAdded = trackedBytesField.getLong(billingService);
                assertEquals(finalAddedBytes, trackedBytesAdded);
                vertxTestContext.completeNow();
            });
        });

    }

    @Test
    void testRemoveStorageBytes(Vertx vertx, VertxTestContext vertxTestContext){

        long removedBytes = 0;
        int runs = 10;

        for(int i = 0; i < runs; i++){
            byte[] buffer = new byte[RandomUtil.generateRandomIntegerValue(50,2048)];
            removedBytes += MemoryUtil.getDeepObjectSize(buffer);
            billingService.removeStorageBytes(buffer);
        }

        long finalRemovedBytes = removedBytes;
        waitForPendingEvents(vertx).onComplete(res -> {
            vertxTestContext.verify(() -> {
                Field trackedBytesField = billingService.getClass().getDeclaredField("removedStorageBytesCurrentTrackingGranularity");
                trackedBytesField.setAccessible(true);
                long trackedBytesAdded = trackedBytesField.getLong(billingService);
                assertEquals(finalRemovedBytes, trackedBytesAdded);
                vertxTestContext.completeNow();
            });
        });

    }



    @Test
    void testChangeStorageBytes(Vertx vertx, VertxTestContext vertxTestContext){

        long addedBytes = 0;
        long removedBytes = 0;
        int runs = 10;

        for(int i = 0; i < runs; i++){
            byte[] bufferBefore = new byte[RandomUtil.generateRandomIntegerValue(50,2048)];
            long sizeBefore = MemoryUtil.getDeepObjectSize(bufferBefore);

            byte[] bufferAfter = new byte[RandomUtil.generateRandomIntegerValue(50,2048)];
            long sizeAfter = MemoryUtil.getDeepObjectSize(bufferAfter);

            billingService.changeStorageBytes(bufferBefore, bufferAfter);

            long diff = sizeAfter - sizeBefore;

            if(diff > 0)
                addedBytes += diff;
            else
                removedBytes += Math.abs(diff);
        }

        long finalAddedBytes = addedBytes;
        long finalRemovedBytes = removedBytes;
        waitForPendingEvents(vertx).onComplete(res -> {
            vertxTestContext.verify(() -> {
                Field trackedBytesField = billingService.getClass().getDeclaredField("totalStorageBytesCurrentTrackingGranularity");
                trackedBytesField.setAccessible(true);
                long trackedBytesAdded = trackedBytesField.getLong(billingService);
                assertEquals(finalAddedBytes, trackedBytesAdded);

                Field removedBytesField = billingService.getClass().getDeclaredField("removedStorageBytesCurrentTrackingGranularity");
                removedBytesField.setAccessible(true);
                long trackedBytesRemoved = removedBytesField.getLong(billingService);
                assertEquals(finalRemovedBytes, trackedBytesRemoved);

                vertxTestContext.completeNow();
            });
        });

    }

    @Test
    void testAddActiveKVMillis(Vertx vertx, VertxTestContext vertxTestContext){
        long[] durationsInmillis = RandomUtil.generateRandomLongValues();
        for(long duration : durationsInmillis)
            billingService.addActiveKVMillis(duration);

        waitForPendingEvents(vertx).onComplete( res -> {
            vertxTestContext.verify(() -> {
                assertEquals(RandomUtil.sumLongValues(durationsInmillis), billingService.getActiveKVMillis());
            });
            vertxTestContext.completeNow();
        });
    }

    @Test
    void testTrackingPeriodStorageBytes(Vertx vertx, VertxTestContext vertxTestContext) {
        byte[] addedBuffer = new byte[RandomUtil.generateRandomIntegerValue(50,4096)];
        byte[] removedBuffer = new byte[RandomUtil.generateRandomIntegerValue(10,addedBuffer.length - 10)];

        final long addedBytes = MemoryUtil.getDeepObjectSize(addedBuffer);
        final long removedBytes = MemoryUtil.getDeepObjectSize(removedBuffer);

        billingService.addStorageBytes(addedBuffer);
        billingService.removeStorageBytes(removedBuffer);

        waitForPendingEvents(vertx).onComplete(res -> {
            vertxTestContext.verify(() -> {
                Field trackedBytesField = billingService.getClass().getDeclaredField("totalStorageBytesCurrentTrackingGranularity");
                trackedBytesField.setAccessible(true);
                long trackedBytesAdded = trackedBytesField.getLong(billingService);
                assertEquals(addedBytes, trackedBytesAdded);

                Field removedBytesField = billingService.getClass().getDeclaredField("removedStorageBytesCurrentTrackingGranularity");
                removedBytesField.setAccessible(true);
                long trackedBytesRemoved = removedBytesField.getLong(billingService);
                assertEquals(removedBytes, trackedBytesRemoved);

                Field currentBillingPeriodBytesField = billingService.getClass().getDeclaredField("totalStorageByteUnitsCurrentBillingPeriod");
                currentBillingPeriodBytesField.setAccessible(true);
                long currentBillingPeriodBytes = currentBillingPeriodBytesField.getLong(billingService);
                assertEquals(0, currentBillingPeriodBytes);
            });
        }).onComplete( res -> {

            // Invoke tracking period by hand:
            Method finishTrackingPeriod = null;
            try {
                finishTrackingPeriod = billingService.getClass().getDeclaredMethod("finishTrackingPeriod");
            } catch (NoSuchMethodException e) {
                vertxTestContext.failNow(e);
            }
            finishTrackingPeriod.setAccessible(true);
            try {
                finishTrackingPeriod.invoke(billingService);
            } catch (IllegalAccessException e) {
                vertxTestContext.failNow(e);
            } catch (InvocationTargetException e) {
                vertxTestContext.failNow(e);
            }

            waitForPendingEvents(vertx).onComplete(res2 -> {
                vertxTestContext.verify(() -> {
                    Field trackedBytesField = billingService.getClass().getDeclaredField("totalStorageBytesCurrentTrackingGranularity");
                    trackedBytesField.setAccessible(true);
                    long trackedBytes = trackedBytesField.getLong(billingService);
                    assertEquals(addedBytes - removedBytes, trackedBytes);

                    Field removedBytesField = billingService.getClass().getDeclaredField("removedStorageBytesCurrentTrackingGranularity");
                    removedBytesField.setAccessible(true);
                    long trackedBytesRemoved = removedBytesField.getLong(billingService);
                    assertEquals(0, trackedBytesRemoved);

                    Field currentBillingPeriodBytesField = billingService.getClass().getDeclaredField("totalStorageByteUnitsCurrentBillingPeriod");
                    currentBillingPeriodBytesField.setAccessible(true);
                    long currentBillingPeriodBytes = currentBillingPeriodBytesField.getLong(billingService);
                    assertEquals(addedBytes, currentBillingPeriodBytes);
                    vertxTestContext.completeNow();

                });
            });
        });

    }

    @Test
    void testBillingPeriod(Vertx vertx, VertxTestContext vertxTestContext) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        billingService.addActiveKVMillis(100);

        billingService.addTcpRequest(TCPRequestType.GET);
        billingService.addTcpRequest(TCPRequestType.PUT);
        billingService.addRpcRequest(RPCRequestType.COMMIT);
        billingService.addRpcRequest(RPCRequestType.INVALIDATE);
        billingService.addRpcRequest(RPCRequestType.GET);

        billingService.addRpcDataTransferOut(10, SAME_REGION, SAME_PROVIDER);
        billingService.addRpcDataTransferOut(10, DIFFERENT_REGION, SAME_PROVIDER);
        billingService.addRpcDataTransferOut(10, DIFFERENT_REGION, DIFFERENT_PROVIDER);
        billingService.addRpcDataTransferOut(10, SAME_REGION, DIFFERENT_PROVIDER);
        billingService.addRpcDataTransferIn(10, SAME_REGION, SAME_PROVIDER);
        billingService.addRpcDataTransferIn(10, DIFFERENT_REGION, SAME_PROVIDER);
        billingService.addRpcDataTransferIn(10, DIFFERENT_REGION, DIFFERENT_PROVIDER);
        billingService.addRpcDataTransferIn(10, SAME_REGION, DIFFERENT_PROVIDER);

        billingService.addTcpDataTransferOut(10, SAME_REGION, SAME_PROVIDER);
        billingService.addTcpDataTransferOut(10, DIFFERENT_REGION, SAME_PROVIDER);
        billingService.addTcpDataTransferOut(10, DIFFERENT_REGION, DIFFERENT_PROVIDER);
        billingService.addTcpDataTransferOut(10, SAME_REGION, DIFFERENT_PROVIDER);
        billingService.addTcpDataTransferIn(10, SAME_REGION, SAME_PROVIDER);
        billingService.addTcpDataTransferIn(10, DIFFERENT_REGION, SAME_PROVIDER);
        billingService.addTcpDataTransferIn(10, DIFFERENT_REGION, DIFFERENT_PROVIDER);
        billingService.addTcpDataTransferIn(10, SAME_REGION, DIFFERENT_PROVIDER);

        byte[] addedBuffer1 = new byte[RandomUtil.generateRandomIntegerValue(50, 4096)];
        byte[] removedBuffer1 = new byte[RandomUtil.generateRandomIntegerValue(10, Math.max(40, addedBuffer1.length - 100))];
        final long addedBytes1 = MemoryUtil.getDeepObjectSize(addedBuffer1);
        final long removedBytes1 = MemoryUtil.getDeepObjectSize(removedBuffer1);
        billingService.addStorageBytes(addedBuffer1);
        billingService.removeStorageBytes(removedBuffer1);

        // Invoke tracking period by hand:
        Method finishTrackingPeriod = billingService.getClass().getDeclaredMethod("finishTrackingPeriod");
        finishTrackingPeriod.setAccessible(true);
        finishTrackingPeriod.invoke(billingService);

        byte[] addedBuffer2 = new byte[RandomUtil.generateRandomIntegerValue(50, 4096)];
        byte[] removedBuffer2 = new byte[RandomUtil.generateRandomIntegerValue(10, Math.max(20, addedBuffer1.length - 300))];
        final long addedBytes2 = MemoryUtil.getDeepObjectSize(addedBuffer2);
        final long removedBytes2 = MemoryUtil.getDeepObjectSize(removedBuffer2);
        billingService.addStorageBytes(addedBuffer2);
        billingService.removeStorageBytes(removedBuffer2);

        finishTrackingPeriod.invoke(billingService);

        waitForPendingEvents(vertx).onComplete(res -> {
            vertxTestContext.verify(() -> {
                Field trackedBytesField = billingService.getClass().getDeclaredField("totalStorageBytesCurrentTrackingGranularity");
                trackedBytesField.setAccessible(true);
                long trackedBytesAdded = trackedBytesField.getLong(billingService);
                assertEquals(addedBytes1 - removedBytes1 + addedBytes2 - removedBytes2, trackedBytesAdded);

                Field removedBytesField = billingService.getClass().getDeclaredField("removedStorageBytesCurrentTrackingGranularity");
                removedBytesField.setAccessible(true);
                long trackedBytesRemoved = removedBytesField.getLong(billingService);
                assertEquals(0, trackedBytesRemoved);

                Field currentBillingPeriodBytesField = billingService.getClass().getDeclaredField("totalStorageByteUnitsCurrentBillingPeriod");
                currentBillingPeriodBytesField.setAccessible(true);
                long currentBillingPeriodBytes = currentBillingPeriodBytesField.getLong(billingService);
                assertEquals(addedBytes1 + (addedBytes1-removedBytes1+addedBytes2), currentBillingPeriodBytes);

                assertFalse(billingService.getActiveKVMillis() == 0);
                assertFalse(CommunicationUtil.communicationsAreReset(billingService.getRpcCommunicationMetrics()));
                assertFalse(CommunicationUtil.communicationsAreReset(billingService.getTcpCommunicationMetrics()));

            });
        }).onComplete( res -> {

            // Invoke tracking period by hand:
            Method finishBillingPeriod = null;
            try {
                finishBillingPeriod = billingService.getClass().getDeclaredMethod("finishBillingPeriod");
            } catch (NoSuchMethodException e) {
                vertxTestContext.failNow(e);
            }
            finishBillingPeriod.setAccessible(true);
            try {
                finishBillingPeriod.invoke(billingService);
            } catch (IllegalAccessException e) {
                vertxTestContext.failNow(e);
            } catch (InvocationTargetException e) {
                vertxTestContext.failNow(e);
            }

            waitForPendingEvents(vertx).onComplete(res2 -> {
                vertxTestContext.verify(() -> {
                    Field trackedBytesField = billingService.getClass().getDeclaredField("totalStorageBytesCurrentTrackingGranularity");
                    trackedBytesField.setAccessible(true);
                    long trackedBytes = trackedBytesField.getLong(billingService);
                    assertEquals(addedBytes1 - removedBytes1 + addedBytes2 - removedBytes2, trackedBytes);

                    Field removedBytesField = billingService.getClass().getDeclaredField("removedStorageBytesCurrentTrackingGranularity");
                    removedBytesField.setAccessible(true);
                    long trackedBytesRemoved = removedBytesField.getLong(billingService);
                    assertEquals(0, trackedBytesRemoved);

                    Field currentBillingPeriodBytesField = billingService.getClass().getDeclaredField("totalStorageByteUnitsCurrentBillingPeriod");
                    currentBillingPeriodBytesField.setAccessible(true);
                    long currentBillingPeriodBytes = currentBillingPeriodBytesField.getLong(billingService);
                    assertEquals(0, currentBillingPeriodBytes);

                    assertTrue(billingService.getActiveKVMillis() == 0);
                    assertTrue(CommunicationUtil.communicationsAreReset(billingService.getRpcCommunicationMetrics()));
                    assertTrue(CommunicationUtil.communicationsAreReset(billingService.getTcpCommunicationMetrics()));

                    vertxTestContext.completeNow();
                });
            });
        });
    }

    private void testAddTcpRequests(TCPRequestType requestType, Vertx vertx, VertxTestContext vertxTestContext){
        long requests = RandomUtil.generateRandomLongValue();
        for(int i = 0; i < requests; i++)
            billingService.addTcpRequest(requestType);

        waitForPendingEvents(vertx).onComplete( res -> {
            vertxTestContext.verify(() -> {
                assertEquals(requests, billingService.getTcpCommunicationMetrics().getNumberOfRequests().get(requestType));
                Arrays.stream(TCPRequestType.values())
                        .filter(type -> type != requestType)
                        .forEach(request -> {
                                    assertEquals(0L, billingService.getTcpCommunicationMetrics().getNumberOfRequests().get(request));
                                }
                        );
            });
            vertxTestContext.completeNow();
        });
    }

    private void testAddRpcRequests(RPCRequestType requestType, Vertx vertx, VertxTestContext vertxTestContext){
        long requests = RandomUtil.generateRandomLongValue();
        for(int i = 0; i < requests; i++)
            billingService.addRpcRequest(requestType);

        waitForPendingEvents(vertx).onComplete( res -> {
            vertxTestContext.verify(() -> {
                assertEquals(requests, billingService.getRpcCommunicationMetrics().getNumberOfRequests().get(requestType));
                Arrays.stream(RPCRequestType.values())
                        .filter(type -> type != requestType)
                        .forEach(request -> {
                                    assertEquals(0L, billingService.getRpcCommunicationMetrics().getNumberOfRequests().get(request));
                                }
                        );
            });
            vertxTestContext.completeNow();
        });
    }

    // Wait for pending events in vertx Worker Pool
    private Future<Object> waitForPendingEvents(Vertx vertx){
        return vertx.executeBlocking(future -> {
            future.complete();
        });
    }
}
