package at.uibk.dps.dml.node.billing;

import at.uibk.dps.dml.node.membership.DataTransferPricingCategory;

import java.util.Arrays;
import java.util.EnumMap;

public abstract class CommunicationMetrics {

    private EnumMap<DataTransferPricingCategory, Long> dataTransferOut;

    private EnumMap<DataTransferPricingCategory, Long> dataTransferIn;

    public CommunicationMetrics() {
        this.dataTransferOut = new EnumMap<>(DataTransferPricingCategory.class);
        this.dataTransferIn = new EnumMap<>(DataTransferPricingCategory.class);
        Arrays.stream(DataTransferPricingCategory.values()).forEach(type -> {
            dataTransferIn.put(type, 0L);
            dataTransferOut.put(type, 0L);
        });

    }

    public abstract EnumMap getNumberOfRequests();

    public abstract void setNumberOfRequests(EnumMap numberOfRequests);

    public abstract long getNumberOfRequests(RequestType requestType);

    public abstract void addRequests(RequestType requestType, long count);

    public abstract void addRequest(RequestType requestType);

    public EnumMap<DataTransferPricingCategory, Long> getDataTransferOut() {
        return dataTransferOut;
    }

    public EnumMap<DataTransferPricingCategory, Long> getDataTransferIn() {
        return dataTransferIn;
    }

    public void addDataTransferOut(DataTransferPricingCategory dataTransferPricingCategory, long sizeInBytes) {
        long currentSizeInBytes = dataTransferOut.get(dataTransferPricingCategory);
        dataTransferOut.put(dataTransferPricingCategory, Math.addExact(currentSizeInBytes, sizeInBytes));
    }

    public void addDataTransferIn(DataTransferPricingCategory dataTransferPricingCategory, long sizeInBytes) {
        long currentSizeInBytes = dataTransferIn.get(dataTransferPricingCategory);
        dataTransferIn.put(dataTransferPricingCategory, Math.addExact(currentSizeInBytes, sizeInBytes));
    }

    public void setDataTransferOut(EnumMap<DataTransferPricingCategory, Long> dataTransferOut) {
        this.dataTransferOut = dataTransferOut;
    }

    public void setDataTransferIn(EnumMap<DataTransferPricingCategory, Long> dataTransferIn) {
        this.dataTransferIn = dataTransferIn;
    }

    protected void resetToZero() {
        Arrays.stream(DataTransferPricingCategory.values()).forEach(type -> {
            dataTransferIn.put(type, 0L);
            dataTransferOut.put(type, 0L);
        });
    }

}
