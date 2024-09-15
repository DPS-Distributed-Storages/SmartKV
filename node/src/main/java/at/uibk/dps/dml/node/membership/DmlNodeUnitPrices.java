package at.uibk.dps.dml.node.membership;

import java.io.Serializable;

/**
 * Specifies the unit prices for the storage node's pricing model.
 */
public class DmlNodeUnitPrices implements Serializable {

    /**
     * The price per GB second of storage in $
     */
    private double unitStoragePrice;

    /**
     * The price per PUT request in $
     */
    private double unitRequestPricePUT;

    /**
     * The price per GET request in $
     */
    private double unitRequestPriceGET;

    /**
     * The price per ms of active KV invocation $
     */
    private double unitActiveKVPrice;

    /**
     * The price per GB of egress data in $ depending on the destination
     */
    private DmlNodeUnitTransferPrices unitEgressTransferPrices;

    /**
     * The price per GB of ingress data in $ depending on the destination
     */
    private DmlNodeUnitTransferPrices unitIngressTransferPrices;


    /**
     *
     * @param unitStoragePrice {@link DmlNodeUnitPrices#unitStoragePrice}
     * @param unitRequestPricePUT {@link DmlNodeUnitPrices#unitRequestPricePUT}
     * @param unitRequestPriceGET {@link DmlNodeUnitPrices#unitRequestPriceGET}
     * @param unitEgressTransferPrices {@link DmlNodeUnitPrices#unitEgressTransferPrices}
     * @param unitIngressTransferPrices {@link DmlNodeUnitPrices#unitIngressTransferPrices}
     * @throws IllegalArgumentException If any of the provided values are not positive
     *
     */
    public DmlNodeUnitPrices(double unitStoragePrice, double unitRequestPricePUT, double unitRequestPriceGET, double unitActiveKVPrice, DmlNodeUnitTransferPrices unitEgressTransferPrices, DmlNodeUnitTransferPrices unitIngressTransferPrices) {
        if (unitStoragePrice < 0 || unitRequestPricePUT < 0 || unitRequestPriceGET < 0 || unitActiveKVPrice < 0) {
            throw new IllegalArgumentException("Prices must be non-negative values!");
        }
        this.unitStoragePrice = unitStoragePrice;
        this.unitRequestPricePUT = unitRequestPricePUT;
        this.unitRequestPriceGET = unitRequestPriceGET;
        this.unitActiveKVPrice = unitActiveKVPrice;
        this.unitEgressTransferPrices = unitEgressTransferPrices;
        this.unitIngressTransferPrices = unitIngressTransferPrices;
    }

    public DmlNodeUnitPrices() {
        this.unitStoragePrice = 0.0;
        this.unitRequestPricePUT = 0.0;
        this.unitRequestPriceGET = 0.0;
        this.unitActiveKVPrice = 0.0;
        this.unitEgressTransferPrices = new DmlNodeUnitTransferPrices();
        this.unitIngressTransferPrices = new DmlNodeUnitTransferPrices();
    }

    /**
     * Returns the {@link DmlNodeUnitPrices#unitStoragePrice}
     * @return {@link DmlNodeUnitPrices#unitStoragePrice}
     */
    public double getUnitStoragePrice() {
        return unitStoragePrice;
    }

    /**
     * Sets the {@link DmlNodeUnitPrices#unitStoragePrice} to the specified price.
     * Ensures that the provided value is positive.
     *
     * @param unitStoragePrice The unit storage price to set
     * @throws IllegalArgumentException If the provided value is not positive
     */
    public void setUnitStoragePrice(double unitStoragePrice) {
        if (unitStoragePrice >= 0) {
            this.unitStoragePrice = unitStoragePrice;
        } else {
            throw new IllegalArgumentException("Unit storage price must be a non-negative value.");
        }
    }

    /**
     * Returns the {@link DmlNodeUnitPrices#unitRequestPricePUT}
     * @return {@link DmlNodeUnitPrices#unitRequestPricePUT}
     */
    public double getUnitRequestPricePUT() {
        return unitRequestPricePUT;
    }

    /**
     * Sets the {@link DmlNodeUnitPrices#unitRequestPricePUT} to the specified price.
     * Ensures that the provided value is positive.
     *
     * @param unitRequestPricePUT The unit request price (PUT) to set
     * @throws IllegalArgumentException If the provided value is not positive
     */
    public void setUnitRequestPricePUT(double unitRequestPricePUT) {
        if (unitRequestPricePUT >= 0) {
            this.unitRequestPricePUT = unitRequestPricePUT;
        } else {
            throw new IllegalArgumentException("Unit request price (PUT) must be a non-negative value.");
        }
    }

    /**
     * Returns the {@link DmlNodeUnitPrices#unitRequestPriceGET}
     * @return {@link DmlNodeUnitPrices#unitRequestPriceGET}
     */
    public double getUnitRequestPriceGET() {
        return unitRequestPriceGET;
    }

    /**
     * Sets the {@link DmlNodeUnitPrices#unitRequestPriceGET} to the specified price.
     * Ensures that the provided value is positive.
     *
     * @param unitRequestPriceGET The unit request price (GET) to set
     * @throws IllegalArgumentException If the provided value is not positive
     */
    public void setUnitRequestPriceGET(double unitRequestPriceGET) {
        if (unitRequestPriceGET >= 0) {
            this.unitRequestPriceGET = unitRequestPriceGET;
        } else {
            throw new IllegalArgumentException("Unit request price (GET) must be a non-negative value.");
        }
    }

    /**
     * Returns the {@link DmlNodeUnitPrices#unitActiveKVPrice}
     * @return {@link DmlNodeUnitPrices#unitActiveKVPrice}
     */
    public double getUnitActiveKVPrice() {
        return unitActiveKVPrice;
    }

    /**
     * Sets the {@link DmlNodeUnitPrices#unitActiveKVPrice} to the specified price.
     * Ensures that the provided value is positive.
     *
     * @param unitActiveKVPrice The unit active KV price to set
     * @throws IllegalArgumentException If the provided value is not positive
     */
    public void setUnitActiveKVPrice(double unitActiveKVPrice) {
        if (unitActiveKVPrice >= 0) {
            this.unitActiveKVPrice = unitActiveKVPrice;
        } else {
            throw new IllegalArgumentException("Unit active KV price must be a non-negative value.");
        }
    }

    /**
     * Returns the {@link DmlNodeUnitPrices#unitEgressTransferPrices}
     * @return {@link DmlNodeUnitPrices#unitEgressTransferPrices}
     */
    public DmlNodeUnitTransferPrices getUnitEgressTransferPrices() {
        return unitEgressTransferPrices;
    }
    /**
     * Sets the {@link DmlNodeUnitPrices#unitEgressTransferPrices} to the specified prices
     * @param unitEgressTransferPrices
     */
    public void setUnitEgressTransferPrices(DmlNodeUnitTransferPrices unitEgressTransferPrices) {
        this.unitEgressTransferPrices = unitEgressTransferPrices;
    }

    /**
     * Returns the {@link DmlNodeUnitPrices#unitIngressTransferPrices}
     * @return {@link DmlNodeUnitPrices#unitIngressTransferPrices}
     */
    public DmlNodeUnitTransferPrices getUnitIngressTransferPrices() {
        return unitIngressTransferPrices;
    }
    /**
     * Sets the {@link DmlNodeUnitPrices#unitIngressTransferPrices} to the specified prices
     * @param unitIngressTransferPrices
     */
    public void setUnitIngressTransferPrices(DmlNodeUnitTransferPrices unitIngressTransferPrices) {
        this.unitIngressTransferPrices = unitIngressTransferPrices;
    }

    @Override
    public String toString() {
        return "DmlNodeUnitPrices{" +
                "unitStoragePrice=" + unitStoragePrice +
                ", unitRequestPricePUT=" + unitRequestPricePUT +
                ", unitRequestPriceGET=" + unitRequestPriceGET +
                ", unitActiveKVPrice=" + unitActiveKVPrice +
                ", unitEgressTransferPrices=" + unitEgressTransferPrices +
                ", unitIngressTransferPrices=" + unitIngressTransferPrices +
                '}';
    }

    public void copyFrom(DmlNodeUnitPrices other){
        this.unitStoragePrice = other.unitStoragePrice;
        this.unitActiveKVPrice = other.unitActiveKVPrice;
        this.unitRequestPriceGET = other.unitRequestPriceGET;
        this.unitRequestPricePUT = other.unitRequestPricePUT;
        this.unitEgressTransferPrices.setUnitTransferPrice(other.unitEgressTransferPrices.getUnitTransferPrice().clone());
        this.unitIngressTransferPrices.setUnitTransferPrice(other.unitIngressTransferPrices.getUnitTransferPrice().clone());
    }

}
