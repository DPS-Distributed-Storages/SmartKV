package at.uibk.dps.dml.node.billing;

import at.uibk.dps.dml.node.membership.*;
import at.uibk.dps.dml.node.util.MemoryUtil;
import at.uibk.dps.dml.node.util.NodeLocation;
import io.micrometer.core.instrument.*;
import io.vertx.core.Vertx;
import io.vertx.micrometer.backends.BackendRegistries;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class BillingService {

    private final NodeLocation localLocation;

    private final DmlNodeUnitPrices dmlNodeUnitPrices;

    private MembershipView membershipView;

    private final TCPCommunicationMetrics tcpCommunicationMetrics;
    private final RPCCommunicationMetrics rpcCommunicationMetrics;

    private final int verticleId;

    /**
     * The granularity in which the stored bytes are tracked (default = every hour)
     */
    private final int bytesTrackingGranularityInMillis;

    /**
     * The granularity in which the stored bytes are billed (default = every day)
     */
    private final int bytesBillingGranularityInMillis;

    /**
     * The total amount of bytes (B) stored per tracking granularity accumulated over the specified billing granularity.
     */
    private long totalStorageByteUnitsCurrentBillingPeriod;

    /**
     * Tracks the amount of bytes (B) in the current tracking period
     */
    private long totalStorageBytesCurrentTrackingGranularity;

    /**
     * Tracks the amount of bytes (B) deleted in the current tracking period
     */
    private long removedStorageBytesCurrentTrackingGranularity;

    private long activeKVMillis;

    private final Vertx vertx;

    private final List<Bill> bills;

    // This will be convenient for benchmarking purposes!
    private double totalBillSinceStart;
    Counter counter;

    private final MeterRegistry registry;

    private MultiGauge billDataTransferMetricsGauge;

    private MultiGauge billRequestCountMetricsGauge;

    private MultiGauge billUnitTransferPriceMetricsGauge;

    private MultiGauge billUnitRequestPriceMetricsGauge;

    private AtomicLong activeKVMillisGauge;

    private AtomicLong storageBytesGauge;

    public BillingService(Vertx vertx, MembershipManager membershipManager, String localRegion, String localProvider, int verticleId, DmlNodeUnitPrices dmlNodeUnitPrices, int bytesTrackingGranularityInMillis, int bytesBillingGranularityInMillis) {
        this.localLocation = new NodeLocation(localRegion, "", localProvider); // Zone is not required for billing!
        this.dmlNodeUnitPrices = dmlNodeUnitPrices;
        this.tcpCommunicationMetrics = new TCPCommunicationMetrics();
        this.rpcCommunicationMetrics = new RPCCommunicationMetrics();
        this.verticleId = verticleId;
        this.vertx = vertx;
        this.totalStorageByteUnitsCurrentBillingPeriod = 0;
        this.totalStorageBytesCurrentTrackingGranularity = 0;
        this.activeKVMillis = 0;
        this.bytesTrackingGranularityInMillis = bytesTrackingGranularityInMillis;
        this.bytesBillingGranularityInMillis = bytesBillingGranularityInMillis;
        this.bills = new ArrayList<>();
        this.totalBillSinceStart = 0.0;

        membershipManager.addListener(view -> vertx.runOnContext(event -> onMembershipChange(view)));
        onMembershipChange(membershipManager.getMembershipView());

        // Tracks the storage bytes per tracking time unit (e.g. byte-hours)
        vertx.setPeriodic(bytesTrackingGranularityInMillis, event -> {
            this.finishTrackingPeriod();
        });

        // Tracks the storage bytes per billing time unit (e.g. byte-hours in the whole month)
        vertx.setPeriodic(bytesBillingGranularityInMillis, event -> {
            this.finishBillingPeriod();
        });

        registry = BackendRegistries.getDefaultNow();

        if (registry != null) {
            billDataTransferMetricsGauge = MultiGauge.builder("sdkv_bill_data_transfer_gauge")
                    .description("Metrics about the TCP and RPC data transfer of the latest finished billing period.")
                    .register(registry);

            billRequestCountMetricsGauge = MultiGauge.builder("sdkv_bill_request_count_gauge")
                    .description("Metrics about the number of TCP and RPC requests of the latest finished billing period.")
                    .register(registry);

            billUnitTransferPriceMetricsGauge = MultiGauge.builder("sdkv_bill_unit_transfer_price_gauge")
                    .description("Metrics about the unit transfer prices of the latest finished billing period.")
                    .register(registry);

            billUnitRequestPriceMetricsGauge = MultiGauge.builder("sdkv_bill_unit_request_price_gauge")
                    .description("Metrics about the unit request prices of the latest finished billing period.")
                    .register(registry);

            activeKVMillisGauge = registry.gauge("sdkv_bill_active_kv_millis_gauge", Tags.of("verticle_id", Integer.toString(verticleId)), new AtomicLong(0));

            storageBytesGauge = registry.gauge("sdkv_bill_storage_bytes_gauge", Tags.of("verticle_id", Integer.toString(verticleId)), new AtomicLong(0));

            Gauge
                    .builder("sdkv_bill_unit_storage_price_gauge", () -> dmlNodeUnitPrices.getUnitStoragePrice())
                    .description("The unit storage price of the latest finished billing period.")
                    .tags("verticle_id", Integer.toString(verticleId))
                    .register(registry);

            Gauge
                    .builder("sdkv_bill_unit_active_kv_price_gauge", () -> dmlNodeUnitPrices.getUnitActiveKVPrice())
                    .description("The unit active kv price of the latest finished billing period.")
                    .tags("verticle_id", Integer.toString(verticleId))
                    .register(registry);

            counter = registry.counter("bill_total_cost_since_start_counter", "verticle_id", Integer.toString(verticleId));

        }
    }


    /**
     * Same as {@link #BillingService(Vertx, MembershipManager, String, String, int, DmlNodeUnitPrices, int, int)} but with default tracking granularity set to 5 seconds and billing period set to 30 seconds.
     */
    public BillingService(Vertx vertx, MembershipManager membershipManager, String localRegion, String localProvider, int verticleId, DmlNodeUnitPrices dmlNodeUnitPrices) {
        this(vertx, membershipManager, localRegion, localProvider, verticleId, dmlNodeUnitPrices, 5 * 1000, 30 * 1000);
    }

    private void finishTrackingPeriod() {
        // We use executeBlocking to ensure that all previous blocking calls are completed before
        vertx.executeBlocking(future -> {
            // Add the current storage byte units (e.g. byte hours) to the billed bytes
            // Each byte which was stored during this period (e.g. hour) will be tracked and charged at full costs (e.g. per hour)
            totalStorageByteUnitsCurrentBillingPeriod = Math.addExact(totalStorageByteUnitsCurrentBillingPeriod, totalStorageBytesCurrentTrackingGranularity);

            // Subtract all bytes removed from the current tracker to get the real current size in bytes
            totalStorageBytesCurrentTrackingGranularity = Math.subtractExact(totalStorageBytesCurrentTrackingGranularity, removedStorageBytesCurrentTrackingGranularity);
            if (totalStorageBytesCurrentTrackingGranularity < 0) {
                totalStorageBytesCurrentTrackingGranularity = 0;
            }

            // Reset the tracker for removed storage bytes
            removedStorageBytesCurrentTrackingGranularity = 0;
            future.complete();
        });
    }

    private void finishBillingPeriod() {
        finishTrackingPeriod();
        // We use executeBlocking to ensure that all previous blocking calls are completed before
        vertx.executeBlocking(future -> {
            // The bill for this pricing period (e.g. month) is finished
            // Charge it
            Bill bill = new Bill(tcpCommunicationMetrics, rpcCommunicationMetrics, totalStorageByteUnitsCurrentBillingPeriod, activeKVMillis, dmlNodeUnitPrices);
            bills.add(bill);
            double totalCost = bill.calculateTotalCosts();
            totalBillSinceStart += totalCost;
            counter.increment(totalCost);

            // Monitor the current Bill
            if (registry != null)
                monitorBill(bill);

            // Reset metrics to zero to start a new pricing period
            totalStorageByteUnitsCurrentBillingPeriod = 0;
            activeKVMillis = 0;
            tcpCommunicationMetrics.resetToZero();
            rpcCommunicationMetrics.resetToZero();

            future.complete();
        });
    }

    @SuppressWarnings("unchecked")
    private void monitorBill(Bill bill) {
        List<MultiGauge.Row<?>> billDataTransferMetricsRows = bill.getTotalTcpCommunicationMetrics().getDataTransferOut().entrySet()
                .stream()
                .map(row -> MultiGauge.Row.of(Tags.of("verticle_id", Integer.toString(verticleId), "category", "tcp_transfer_out", "otherEnd", row.getKey().name()), row.getValue()))
                .collect(Collectors.toList());

        billDataTransferMetricsRows.addAll(
                bill.getTotalTcpCommunicationMetrics().getDataTransferIn().entrySet()
                        .stream()
                        .map(row -> MultiGauge.Row.of(Tags.of("verticle_id", Integer.toString(verticleId), "category", "tcp_transfer_in", "otherEnd", row.getKey().name()), row.getValue()))
                        .collect(Collectors.toList())
        );

        billDataTransferMetricsRows.addAll(
                bill.getTotalRpcCommunicationMetrics().getDataTransferOut().entrySet()
                        .stream()
                        .map(row -> MultiGauge.Row.of(Tags.of("verticle_id", Integer.toString(verticleId), "category", "rpc_transfer_out", "otherEnd", row.getKey().name()), row.getValue()))
                        .collect(Collectors.toList())
        );

        billDataTransferMetricsRows.addAll(
                bill.getTotalRpcCommunicationMetrics().getDataTransferIn().entrySet()
                        .stream()
                        .map(row -> MultiGauge.Row.of(Tags.of("verticle_id", Integer.toString(verticleId), "category", "rpc_transfer_in", "otherEnd", row.getKey().name()), row.getValue()))
                        .collect(Collectors.toList())
        );


        billDataTransferMetricsGauge.register(billDataTransferMetricsRows, true);

        List<MultiGauge.Row<?>> billRequestMetricsRows = ((EnumMap<TCPRequestType, Long>) bill.getTotalTcpCommunicationMetrics().getNumberOfRequests())
                .entrySet()
                .stream()
                .map(row -> MultiGauge.Row.of(Tags.of("verticle_id", Integer.toString(verticleId), "category", "tcp_number_of_requests", "request_type", row.getKey().name()), row.getValue()))
                .collect(Collectors.toList());

        billRequestMetricsRows.addAll((
                (EnumMap<RPCRequestType, Long>) bill.getTotalRpcCommunicationMetrics().getNumberOfRequests())
                .entrySet()
                .stream()
                .map(row -> MultiGauge.Row.of(Tags.of("verticle_id", Integer.toString(verticleId), "category", "rpc_number_of_requests", "request_type", row.getKey().name()), row.getValue()))
                .collect(Collectors.toList())
        );


        billRequestCountMetricsGauge.register(billRequestMetricsRows, true);


        List<MultiGauge.Row<?>> billUnitTransferPriceMetricsRows = bill.getDmlNodeUnitPrices().getUnitEgressTransferPrices().getUnitTransferPrice()
                .entrySet()
                .stream()
                .map(row -> MultiGauge.Row.of(Tags.of("verticle_id", Integer.toString(verticleId), "category", "unit_egress_transfer_price", "otherEnd", row.getKey().name()), row.getValue()))
                .collect(Collectors.toList());

        billUnitTransferPriceMetricsRows.addAll(
                bill.getDmlNodeUnitPrices().getUnitIngressTransferPrices().getUnitTransferPrice()
                        .entrySet()
                        .stream()
                        .map(row -> MultiGauge.Row.of(Tags.of("verticle_id", Integer.toString(verticleId), "category", "unit_ingress_transfer_price", "otherEnd", row.getKey().name()), row.getValue()))
                        .collect(Collectors.toList())
        );


        billUnitTransferPriceMetricsGauge.register(billUnitTransferPriceMetricsRows, true);

        List<MultiGauge.Row<?>> billUnitRequestPriceMetricsRows = new ArrayList<>();
        billUnitRequestPriceMetricsRows.add(MultiGauge.Row.of(Tags.of("verticle_id", Integer.toString(verticleId), "request_type", "PUT"), bill.getDmlNodeUnitPrices().getUnitRequestPricePUT()));
        billUnitRequestPriceMetricsRows.add(MultiGauge.Row.of(Tags.of("verticle_id", Integer.toString(verticleId), "request_type", "GET"), bill.getDmlNodeUnitPrices().getUnitRequestPriceGET()));

        billUnitRequestPriceMetricsGauge.register(billUnitRequestPriceMetricsRows, true);

        registry.gauge("sdkv_bill_unit_storage_price_gauge", bill.getDmlNodeUnitPrices().getUnitStoragePrice());
        registry.gauge("sdkv_bill_unit_active_kv_price_gauge", bill.getDmlNodeUnitPrices().getUnitStoragePrice());

        activeKVMillisGauge.set(bill.getTotalActiveKVMillis());
        storageBytesGauge.set(bill.getTotalStorageByteUnits());
    }

    public TCPCommunicationMetrics getTcpCommunicationMetrics() {
        return tcpCommunicationMetrics;
    }

    public RPCCommunicationMetrics getRpcCommunicationMetrics() {
        return rpcCommunicationMetrics;
    }


    public NodeLocation getLocalLocation() {
        return localLocation;
    }

    private void onMembershipChange(MembershipView membershipView) {
        if (membershipView == null ||
                (this.membershipView != null && membershipView.getEpoch() <= this.membershipView.getEpoch())) {
            return;
        }
        this.membershipView = membershipView;
    }

    private void addDataTransferIn(CommunicationMetrics communicationMetrics, long sizeInBytes, String remoteRegion, String remoteProvider) {
        communicationMetrics.addDataTransferIn(getDataTransferPricingCategory(remoteRegion, remoteProvider), sizeInBytes);
    }

    private void addDataTransferOut(CommunicationMetrics communicationMetrics, long sizeInBytes, String remoteRegion, String remoteProvider) {
        communicationMetrics.addDataTransferOut(getDataTransferPricingCategory(remoteRegion, remoteProvider), sizeInBytes);
    }

    private void addDataTransferIn(CommunicationMetrics communicationMetrics, long sizeInBytes, int remoteVerticleId) {
        final VerticleInfo remoteVerticle = membershipView.findVerticleById(remoteVerticleId);
        addDataTransferIn(communicationMetrics, sizeInBytes, remoteVerticle.getOwnerNode().getRegion(), remoteVerticle.getOwnerNode().getProvider());
    }

    private void addDataTransferOut(CommunicationMetrics communicationMetrics, long sizeInBytes, int remoteVerticleId) {
        final VerticleInfo remoteVerticle = membershipView.findVerticleById(remoteVerticleId);
        addDataTransferOut(communicationMetrics, sizeInBytes, remoteVerticle.getOwnerNode().getRegion(), remoteVerticle.getOwnerNode().getProvider());
    }


    public void addTcpDataTransferIn(long sizeInBytes, int remoteVerticleId) {
        vertx.executeBlocking(future -> {
            addDataTransferIn(this.tcpCommunicationMetrics, sizeInBytes, remoteVerticleId);
            future.complete();
        });
    }

    public void addRpcDataTransferIn(long sizeInBytes, int remoteVerticleId) {
        vertx.executeBlocking(future -> {
            addDataTransferIn(this.rpcCommunicationMetrics, sizeInBytes, remoteVerticleId);
            future.complete();
        });
    }

    public void addTcpDataTransferOut(long sizeInBytes, int remoteVerticleId) {
        vertx.executeBlocking(future -> {
            addDataTransferOut(this.tcpCommunicationMetrics, sizeInBytes, remoteVerticleId);
            future.complete();
        });
    }

    public void addRpcDataTransferOut(long sizeInBytes, int remoteVerticleId) {
        vertx.executeBlocking(future -> {
            addDataTransferOut(this.rpcCommunicationMetrics, sizeInBytes, remoteVerticleId);
            future.complete();
        });
    }

    public void addTcpDataTransferIn(long sizeInBytes, NodeLocation clientLocation) {
        addTcpDataTransferIn(sizeInBytes, clientLocation.getRegion(), clientLocation.getProvider());
    }

    public void addTcpDataTransferIn(long sizeInBytes, String remoteRegion, String remoteProvider) {
        vertx.executeBlocking(future -> {
            addDataTransferIn(this.tcpCommunicationMetrics, sizeInBytes, remoteRegion, remoteProvider);
            future.complete();
        });
    }

    public void addRpcDataTransferIn(long sizeInBytes, String remoteRegion, String remoteProvider) {
        vertx.executeBlocking(future -> {
            addDataTransferIn(this.rpcCommunicationMetrics, sizeInBytes, remoteRegion, remoteProvider);
            future.complete();
        });
    }

    public void addTcpDataTransferOut(long sizeInBytes, NodeLocation clientLocation) {
        addTcpDataTransferOut(sizeInBytes, clientLocation.getRegion(), clientLocation.getProvider());
    }

    public void addTcpDataTransferOut(long sizeInBytes, String remoteRegion, String remoteProvider) {
        vertx.executeBlocking(future -> {
            addDataTransferOut(this.tcpCommunicationMetrics, sizeInBytes, remoteRegion, remoteProvider);
            future.complete();
        });
    }

    public void addRpcDataTransferOut(long sizeInBytes, String remoteRegion, String remoteProvider) {
        vertx.executeBlocking(future -> {
            addDataTransferOut(this.rpcCommunicationMetrics, sizeInBytes, remoteRegion, remoteProvider);
            future.complete();
        });
    }

    private DataTransferPricingCategory getDataTransferPricingCategory(String remoteRegion, String remoteProvider) {
        if (localLocation.getRegion().equals(remoteRegion) && localLocation.getProvider().equals(remoteProvider)) {
            return DataTransferPricingCategory.SAME_REGION;
        } else if (localLocation.getProvider().equals(remoteProvider)) {
            return DataTransferPricingCategory.SAME_PROVIDER;
        } else return DataTransferPricingCategory.INTERNET;
    }

    public void addTcpRequest(TCPRequestType tcpRequestType) {
        vertx.executeBlocking(future -> {
            this.tcpCommunicationMetrics.addRequest(tcpRequestType);
            future.complete();
        });
    }

    public void addRpcRequest(RPCRequestType rpcRequestType) {
        vertx.executeBlocking(future -> {
            this.rpcCommunicationMetrics.addRequest(rpcRequestType);
            future.complete();
        });
    }

    private void addStorageBytes(long sizeInBytes) {
        if (sizeInBytes < 0) {
            removedStorageBytesCurrentTrackingGranularity = Math.addExact(removedStorageBytesCurrentTrackingGranularity, sizeInBytes);
        } else {
            this.totalStorageBytesCurrentTrackingGranularity = Math.addExact(totalStorageBytesCurrentTrackingGranularity, sizeInBytes);
        }
    }

    private void removeStorageBytes(long sizeInBytes) {
        removedStorageBytesCurrentTrackingGranularity = Math.addExact(removedStorageBytesCurrentTrackingGranularity, sizeInBytes);
    }

    private void _changeStorageBytes(long oldSizeInBytes, long newSizeInBytes) {
        long difference = Math.subtractExact(newSizeInBytes, oldSizeInBytes);
        if (difference >= 0) {
            addStorageBytes(difference);
        } else {
            removeStorageBytes(Math.abs(difference));
        }
    }

    public void changeStorageBytes(long oldSizeInBytes, long newSizeInBytes) {
        vertx.executeBlocking(future -> {
            _changeStorageBytes(oldSizeInBytes, newSizeInBytes);
            future.complete();
        });
    }

    public void changeStorageBytes(Object objectBefore, Object objectAfter) {
        vertx.executeBlocking(future -> {
            long oldSizeInBytes = MemoryUtil.getDeepObjectSize(objectBefore);
            long newSizeInBytes = MemoryUtil.getDeepObjectSize(objectAfter);
            _changeStorageBytes(oldSizeInBytes, newSizeInBytes);
            future.complete();
        });
    }

    public void addStorageBytes(Object object) {
        vertx.executeBlocking(future -> {
            addStorageBytes(MemoryUtil.getDeepObjectSize(object));
            future.complete();
        });
    }

    public void removeStorageBytes(Object object) {
        vertx.executeBlocking(future -> {
            removeStorageBytes(MemoryUtil.getDeepObjectSize(object));
            future.complete();
        });
    }

    public void addActiveKVMillis(long durationInMillis) {
        vertx.executeBlocking(future -> {
            activeKVMillis = Math.addExact(activeKVMillis, durationInMillis);
            future.complete();
        });
    }

    public long getActiveKVMillis() {
        return activeKVMillis;
    }

    public DmlNodeUnitPrices getDmlNodeUnitPrices() {
        return dmlNodeUnitPrices;
    }

    // Finishes the current bill and returns the total costs of all bills
    public List<Double> getBills() {
        List<Double> totalBills = new ArrayList<>();
        for (Bill bill : this.bills) {
            totalBills.add(bill.calculateTotalCosts());
        }
        return totalBills;
    }

}