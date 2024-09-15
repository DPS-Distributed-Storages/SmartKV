package at.uibk.dps.dml.node.util;

import at.uibk.dps.dml.node.membership.DmlNodeUnitPrices;

import java.io.Serializable;

/**
 * Wrapper class for information about a zone required for the CELL optimizer
 */
public class ZoneInfo implements Serializable {

    // The average prices per zone weighted over all storage node prices and the current free memory on each storage node.
    private final DmlNodeUnitPrices weightedAverageUnitPrices;

    // The total currently free memory in the zone in Bytes
    private final long totalFreeMemory;

    public ZoneInfo(DmlNodeUnitPrices weightedAverageUnitPrices, long totalFreeMemory) {
        this.weightedAverageUnitPrices = weightedAverageUnitPrices;
        this.totalFreeMemory = totalFreeMemory;
    }

    public DmlNodeUnitPrices getWeightedAverageUnitPrices() {
        return weightedAverageUnitPrices;
    }

    public long getTotalFreeMemory() {
        return totalFreeMemory;
    }
}
