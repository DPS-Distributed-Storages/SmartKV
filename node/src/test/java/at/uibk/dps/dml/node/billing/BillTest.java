package at.uibk.dps.dml.node.billing;

import at.uibk.dps.dml.node.membership.DataTransferPricingCategory;
import at.uibk.dps.dml.node.membership.DmlNodeUnitPrices;
import at.uibk.dps.dml.node.membership.DmlNodeUnitTransferPrices;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class BillTest {

    private DmlNodeUnitPrices dmlNodeUnitPrices;

    private TCPCommunicationMetrics tcpCommunicationMetrics;
    private RPCCommunicationMetrics rpcCommunicationMetrics;
    private final int requestUnits = 100;

    @BeforeEach
    void beforeEach() {

        tcpCommunicationMetrics = new TCPCommunicationMetrics();
        tcpCommunicationMetrics.addRequests(TCPRequestType.PUT, 5);
        tcpCommunicationMetrics.addRequests(TCPRequestType.GET, 10);

        EnumMap<DataTransferPricingCategory, Long> tcpDataTransferOut = new EnumMap<>(
                Map.of(
                        DataTransferPricingCategory.SAME_PROVIDER, 1L,
                        DataTransferPricingCategory.SAME_REGION, 2L,
                        DataTransferPricingCategory.INTERNET, 3L
                )
        );

        EnumMap<DataTransferPricingCategory, Long> tcpDataTransferIn = new EnumMap<>(
                Map.of(
                        DataTransferPricingCategory.SAME_PROVIDER, 3L,
                        DataTransferPricingCategory.SAME_REGION, 4L,
                        DataTransferPricingCategory.INTERNET, 5L
                )
        );

        tcpCommunicationMetrics.setDataTransferOut(tcpDataTransferOut);
        tcpCommunicationMetrics.setDataTransferIn(tcpDataTransferIn);

        rpcCommunicationMetrics = new RPCCommunicationMetrics();
        rpcCommunicationMetrics.addRequests(RPCRequestType.GET, 15);
        rpcCommunicationMetrics.addRequests(RPCRequestType.COMMIT, 8);
        rpcCommunicationMetrics.addRequests(RPCRequestType.INVALIDATE, 3);

        EnumMap<DataTransferPricingCategory, Long> rpcDataTransferOut = new EnumMap<>(
                Map.of(
                        DataTransferPricingCategory.SAME_PROVIDER, 5L,
                        DataTransferPricingCategory.SAME_REGION, 6L,
                        DataTransferPricingCategory.INTERNET, 7L
                )
        );

        EnumMap<DataTransferPricingCategory, Long> rpcDataTransferIn = new EnumMap<>(
                Map.of(
                        DataTransferPricingCategory.SAME_PROVIDER, 7L,
                        DataTransferPricingCategory.SAME_REGION, 8L,
                        DataTransferPricingCategory.INTERNET, 9L
                )
        );

        rpcCommunicationMetrics.setDataTransferOut(rpcDataTransferOut);
        rpcCommunicationMetrics.setDataTransferIn(rpcDataTransferIn);

        dmlNodeUnitPrices = new DmlNodeUnitPrices();
        dmlNodeUnitPrices.setUnitActiveKVPrice(0.9002);
        dmlNodeUnitPrices.setUnitRequestPriceGET(0.5);
        dmlNodeUnitPrices.setUnitRequestPricePUT(0.8);

        DmlNodeUnitTransferPrices egressPrices = new DmlNodeUnitTransferPrices(
                new EnumMap<>(
                        Map.of(
                                DataTransferPricingCategory.SAME_PROVIDER, 1.0,
                                DataTransferPricingCategory.SAME_REGION, 2.0,
                                DataTransferPricingCategory.INTERNET, 4.0
                        )
                )
        );

        DmlNodeUnitTransferPrices ingressPrices = new DmlNodeUnitTransferPrices(
                new EnumMap<>(
                        Map.of(
                                DataTransferPricingCategory.SAME_PROVIDER, 0.5,
                                DataTransferPricingCategory.SAME_REGION, 1.0,
                                DataTransferPricingCategory.INTERNET, 3.0
                        )
                )
        );


        dmlNodeUnitPrices.setUnitEgressTransferPrices(egressPrices);
        dmlNodeUnitPrices.setUnitIngressTransferPrices(ingressPrices);
        dmlNodeUnitPrices.setUnitStoragePrice(0.801);
        dmlNodeUnitPrices.setUnitActiveKVPrice(0.9002);

    }

    @Test
    void testCalculateTotalCosts() {
        Bill bill = new Bill(tcpCommunicationMetrics, rpcCommunicationMetrics, 4096L, 70000L, dmlNodeUnitPrices);

        double totalCosts = bill.calculateTotalCosts();

        // Expected total costs calculation
        double expectedCosts =
                        // TCP Request Costs
                        (10 * 0.5 + 5 * 0.8) / this.requestUnits +
                        // TCP Egress Costs
                        bytesToMegabytes(1.0 * 1.0 + 2.0 * 2.0 + 3.0 * 4.0) +
                        // TCP Ingress Costs
                        bytesToMegabytes(3.0 * 0.5 + 4.0 * 1.0 + 5.0 * 3.0) +
                        // RPC Request Costs
                        (15 * 0.5 + 8 * 0.5 + 3 * 0.8) / this.requestUnits +
                        // RPC Egress Costs
                        bytesToMegabytes(5.0 * 1.0 + 6.0 * 2.0 + 7.0 * 4.0) +
                        // RPC Ingress Costs
                        bytesToMegabytes(7.0 * 0.5 + 8.0 * 1.0 + 9.0 * 3.0) +
                        // Total Storage Byte Costs
                        bytesToMegabytes(4096L * 0.801) +
                        // Total Active KV Costs
                        (70000L * 0.9002);

        assertEquals(expectedCosts, totalCosts, 0.0001);
    }

    // Helper method to create a mock transfer map
    private EnumMap<DataTransferPricingCategory, Long> createMockTransferMap(long value1, long value2, long value3) {
        EnumMap<DataTransferPricingCategory, Long> transferMap = new EnumMap<>(DataTransferPricingCategory.class);
        Arrays.stream(DataTransferPricingCategory.values()).forEach(type -> {
            transferMap.put(type, 0L);
        });
        transferMap.put(DataTransferPricingCategory.SAME_REGION, value1);
        transferMap.put(DataTransferPricingCategory.SAME_PROVIDER, value2);
        transferMap.put(DataTransferPricingCategory.INTERNET, value3);
        return transferMap;
    }

    private DmlNodeUnitTransferPrices createMockTransferPrices(double value1, double value2, double value3) {
        EnumMap<DataTransferPricingCategory, Double> transferMap = new EnumMap<>(DataTransferPricingCategory.class);
        Arrays.stream(DataTransferPricingCategory.values()).forEach(type -> {
            transferMap.put(type, 0.0);
        });
        transferMap.put(DataTransferPricingCategory.SAME_REGION, value1);
        transferMap.put(DataTransferPricingCategory.SAME_PROVIDER, value2);
        transferMap.put(DataTransferPricingCategory.INTERNET, value3);
        return new DmlNodeUnitTransferPrices(transferMap);
    }

    private double bytesToMegabytes(long bytes){
        return bytesToMegabytes((double) bytes);
    }

    private double bytesToMegabytes(double bytes){
        return bytes / (1024 * 1024);
    }
}
