package at.uibk.dps.dml.node.billing;

import at.uibk.dps.dml.node.membership.DmlNodeUnitPrices;

public class Bill {

    private final TCPCommunicationMetrics totalTcpCommunicationMetrics;
    private final RPCCommunicationMetrics totalRpcCommunicationMetrics;
    private final long totalStorageByteUnits;
    private final long totalActiveKVMillis;
    private final DmlNodeUnitPrices dmlNodeUnitPrices;

    // Granularity at which requests are billed (e.g. 1$ per 100 PUT requests)
    private final int requestUnits = 100;

    public Bill(TCPCommunicationMetrics totalTcpCommunicationMetrics, RPCCommunicationMetrics totalRpcCommunicationMetrics, long totalStorageByteUnits, long totalActiveKVMillis, DmlNodeUnitPrices dmlNodeUnitPrices) {
        this.totalTcpCommunicationMetrics = new TCPCommunicationMetrics();
        this.totalTcpCommunicationMetrics.copyFrom(totalTcpCommunicationMetrics);
        this.totalRpcCommunicationMetrics = new RPCCommunicationMetrics();
        this.totalRpcCommunicationMetrics.copyFrom(totalRpcCommunicationMetrics);
        this.totalStorageByteUnits = totalStorageByteUnits;
        this.totalActiveKVMillis = totalActiveKVMillis;
        this.dmlNodeUnitPrices = new DmlNodeUnitPrices();
        this.dmlNodeUnitPrices.copyFrom(dmlNodeUnitPrices);
    }

    protected double calculateTotalCosts(){
        return calculateTcpEgressCosts()
                + calculateTcpIngressCosts()
                + calculateRpcEgressCosts()
                + calculateRpcIngressCosts()
                + calculateTcpRequestCosts()
                + calculateRpcRequestCosts()
                + calculateTotalStorageByteCosts()
                + calculateTotalActiveKVCosts();
    }

    protected double calculateTcpRequestCosts(){
        return (totalTcpCommunicationMetrics.getNumberOfRequests(TCPRequestType.GET) * dmlNodeUnitPrices.getUnitRequestPriceGET()
                + totalTcpCommunicationMetrics.getNumberOfRequests(TCPRequestType.PUT) * dmlNodeUnitPrices.getUnitRequestPricePUT()) / (double) requestUnits;
    }

    protected double calculateTcpEgressCosts(){
        return totalTcpCommunicationMetrics.getDataTransferOut().entrySet().stream()
                .mapToDouble(entry -> entry.getValue() * dmlNodeUnitPrices.getUnitEgressTransferPrices().getUnitTransferPrice().get(entry.getKey()))
                .map(this::bytesToMegabytes)
                .sum();
    }

    protected double calculateTcpIngressCosts(){
        return totalTcpCommunicationMetrics.getDataTransferIn().entrySet().stream()
                .mapToDouble(entry -> entry.getValue() * dmlNodeUnitPrices.getUnitIngressTransferPrices().getUnitTransferPrice().get(entry.getKey()))
                .map(this::bytesToMegabytes)
                .sum();
    }

    protected double calculateRpcRequestCosts(){
        return (totalRpcCommunicationMetrics.getNumberOfRequests(RPCRequestType.GET) * dmlNodeUnitPrices.getUnitRequestPriceGET()
                + totalRpcCommunicationMetrics.getNumberOfRequests(RPCRequestType.COMMIT) * dmlNodeUnitPrices.getUnitRequestPriceGET()
                + totalRpcCommunicationMetrics.getNumberOfRequests(RPCRequestType.INVALIDATE) * dmlNodeUnitPrices.getUnitRequestPricePUT()) / (double) requestUnits;
    }

    protected double calculateRpcEgressCosts(){
        return totalRpcCommunicationMetrics.getDataTransferOut().entrySet().stream()
                .mapToDouble(entry -> entry.getValue() * dmlNodeUnitPrices.getUnitEgressTransferPrices().getUnitTransferPrice().get(entry.getKey()))
                .map(this::bytesToMegabytes)
                .sum();
    }

    protected double calculateRpcIngressCosts(){
        return totalRpcCommunicationMetrics.getDataTransferIn().entrySet().stream()
                .mapToDouble(entry -> entry.getValue() * dmlNodeUnitPrices.getUnitIngressTransferPrices().getUnitTransferPrice().get(entry.getKey()))
                .map(this::bytesToMegabytes)
                .sum();
    }

    protected double calculateTotalStorageByteCosts(){
        return this.bytesToMegabytes(totalStorageByteUnits) * dmlNodeUnitPrices.getUnitStoragePrice();
    }

    protected double calculateTotalActiveKVCosts(){
        return totalActiveKVMillis * dmlNodeUnitPrices.getUnitActiveKVPrice();
    }

    private double bytesToMegabytes(long bytes){
        return bytesToMegabytes((double) bytes);
    }

    // We switch to MB as costs get very small in our experiments with GB conversion
    private double bytesToMegabytes(double bytes){
        return bytes / (1048576); // 1024 * 1024 for MB
    }

    public TCPCommunicationMetrics getTotalTcpCommunicationMetrics() {
        return totalTcpCommunicationMetrics;
    }

    public RPCCommunicationMetrics getTotalRpcCommunicationMetrics() {
        return totalRpcCommunicationMetrics;
    }

    public long getTotalStorageByteUnits() {
        return totalStorageByteUnits;
    }

    public long getTotalActiveKVMillis() {
        return totalActiveKVMillis;
    }

    public DmlNodeUnitPrices getDmlNodeUnitPrices() {
        return dmlNodeUnitPrices;
    }
}
