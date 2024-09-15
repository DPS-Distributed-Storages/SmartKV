package at.uibk.dps.dml.node.billing;

import java.util.Arrays;
import java.util.EnumMap;

public class RPCCommunicationMetrics extends CommunicationMetrics {

    private EnumMap<RPCRequestType, Long> numberOfRequests;

    public RPCCommunicationMetrics() {
        numberOfRequests = new EnumMap<>(RPCRequestType.class);
        Arrays.stream(RPCRequestType.values()).forEach(type -> numberOfRequests.put(type, 0L));
    }

    @SuppressWarnings("unchecked")
    public EnumMap getNumberOfRequests() {
        return numberOfRequests;
    }

    @SuppressWarnings("unchecked")
    public void setNumberOfRequests(EnumMap numberOfRequests) {
        this.numberOfRequests = numberOfRequests;
    }

    public long getNumberOfRequests(RequestType requestType) {
        return numberOfRequests.get(requestType);
    }

    public void addRequests(RequestType requestType, long count) {
        long requests = numberOfRequests.get(requestType);
        numberOfRequests.put((RPCRequestType) requestType, Math.addExact(requests, count));
    }

    public void addRequest(RequestType requestType) {
        long requests = numberOfRequests.get(requestType);
        numberOfRequests.put((RPCRequestType) requestType, Math.addExact(requests, 1));
    }

    protected void resetToZero() {
        super.resetToZero();
        Arrays.stream(RPCRequestType.values()).forEach(type -> numberOfRequests.put(type, 0L));
    }

    protected void copyFrom(RPCCommunicationMetrics other) {
        if (other.getDataTransferIn() != null)
            this.setDataTransferIn(other.getDataTransferIn().clone());
        if (other.getDataTransferOut() != null)
            this.setDataTransferOut(other.getDataTransferOut().clone());
        if (other.getNumberOfRequests() != null)
            this.setNumberOfRequests(other.getNumberOfRequests().clone());
    }

}
