package at.uibk.dps.dml.node.membership;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serializable;
import java.util.Arrays;
import java.util.EnumMap;

/**
 * Specifies the unit prices for data transfer of a storage node's pricing model.
 */
public class DmlNodeUnitTransferPrices implements Serializable {

    /**
     * The price per GB of transferred data in $ depending on the location of sender and receiver
     */
    private EnumMap<DataTransferPricingCategory, Double> unitTransferPrice;

    /**
     * Create a DmlNodeUnitTransferPrices object with specified values:
     * @param unitTransferPrice {@link DmlNodeUnitTransferPrices#unitTransferPrice}
     * @throws IllegalArgumentException If any of the provided prices are not positive
     */
    public DmlNodeUnitTransferPrices(EnumMap<DataTransferPricingCategory, Double> unitTransferPrice) {
        for(Double price : unitTransferPrice.values()) {
            if (price < 0) {
                throw new IllegalArgumentException("Prices must be non-negative values.");
            }
        }
        this.unitTransferPrice = unitTransferPrice;
    }

    public DmlNodeUnitTransferPrices() {
        this.unitTransferPrice = new EnumMap<>(DataTransferPricingCategory.class);
        Arrays.stream(DataTransferPricingCategory.values()).forEach(type -> {
            unitTransferPrice.put(type, 0.0);
        });
    }

    /**
     * Returns the {@link DmlNodeUnitTransferPrices#unitTransferPrice}
     * @return {@link DmlNodeUnitTransferPrices#unitTransferPrice}}
     */
    public EnumMap<DataTransferPricingCategory, Double> getUnitTransferPrice() {
        return unitTransferPrice;
    }

    /**
     * Sets the {@link DmlNodeUnitTransferPrices#unitTransferPrice} to the specified prices.
     * Ensures that the provided prices are positive.
     *
     * @param unitTransferPrice The unit transfer prices to set
     * @throws IllegalArgumentException If any provided prices are not positive
     */
    public void setUnitTransferPrice(EnumMap<DataTransferPricingCategory, Double> unitTransferPrice) {
        for(Double price : unitTransferPrice.values()) {
            if (price < 0) {
                throw new IllegalArgumentException("Prices must be non-negative values.");
            }
        }
        this.unitTransferPrice = unitTransferPrice;
    }

    /**
     * Returns the {@link DmlNodeUnitTransferPrices#unitTransferPrice} for {@link DataTransferPricingCategory#SAME_REGION}
     * @return {@link DmlNodeUnitTransferPrices#unitTransferPrice}} for {@link DataTransferPricingCategory#SAME_REGION}
     */
    @JsonIgnore
    public double getUnitTransferPriceSameRegion() {
        return unitTransferPrice.get(DataTransferPricingCategory.SAME_REGION);
    }

    /**
     * Sets the {@link DmlNodeUnitTransferPrices#unitTransferPrice} to the specified price for {@link DataTransferPricingCategory#SAME_REGION}.
     * Ensures that the provided value is positive.
     *
     * @param unitTransferPriceSameRegion The unit transfer price for {@link DataTransferPricingCategory#SAME_REGION} to set
     * @throws IllegalArgumentException If the provided value is not positive
     */
    public void setUnitTransferPriceSameRegion(double unitTransferPriceSameRegion) {
        if (unitTransferPriceSameRegion >= 0) {
            this.unitTransferPrice.put(DataTransferPricingCategory.SAME_REGION, unitTransferPriceSameRegion);
        } else {
            throw new IllegalArgumentException("Unit transfer price for the same region must be a non-negative value.");
        }
    }

    /**
     * Returns the {@link DmlNodeUnitTransferPrices#unitTransferPrice} for {@link DataTransferPricingCategory#SAME_PROVIDER}
     * @return {@link DmlNodeUnitTransferPrices#unitTransferPrice}} for {@link DataTransferPricingCategory#SAME_PROVIDER}
     */
    @JsonIgnore
    public double getUnitTransferPriceSameProvider() {
        return unitTransferPrice.get(DataTransferPricingCategory.SAME_PROVIDER);
    }

    /**
     * Sets the {@link DmlNodeUnitTransferPrices#unitTransferPrice} to the specified price for {@link DataTransferPricingCategory#SAME_PROVIDER}.
     * Ensures that the provided value is positive.
     *
     * @param unitTransferPriceSameProvider The unit transfer price for {@link DataTransferPricingCategory#SAME_PROVIDER} to set
     * @throws IllegalArgumentException If the provided value is not positive
     */
    public void setUnitTransferPriceSameProvider(double unitTransferPriceSameProvider) {
        if (unitTransferPriceSameProvider >= 0) {
            this.unitTransferPrice.put(DataTransferPricingCategory.SAME_PROVIDER, unitTransferPriceSameProvider);
        } else {
            throw new IllegalArgumentException("Unit transfer price for the same provider must be a non-negative value.");
        }
    }

    /**
     * Returns the {@link DmlNodeUnitTransferPrices#unitTransferPrice} for {@link DataTransferPricingCategory#INTERNET}
     * @return {@link DmlNodeUnitTransferPrices#unitTransferPrice}} for {@link DataTransferPricingCategory#INTERNET}
     */
    @JsonIgnore
    public double getUnitTransferPriceInternet() {
        return unitTransferPrice.get(DataTransferPricingCategory.INTERNET);
    }

    /**
     * Sets the {@link DmlNodeUnitTransferPrices#unitTransferPrice} to the specified price for {@link DataTransferPricingCategory#INTERNET}.
     * Ensures that the provided value is positive.
     *
     * @param unitTransferPriceInternet The unit transfer price for {@link DataTransferPricingCategory#INTERNET} to set
     * @throws IllegalArgumentException If the provided value is not positive
     */
    public void setUnitTransferPriceInternet(double unitTransferPriceInternet) {
        if (unitTransferPriceInternet >= 0) {
            this.unitTransferPrice.put(DataTransferPricingCategory.INTERNET, unitTransferPriceInternet);
        } else {
            throw new IllegalArgumentException("Unit transfer price for internet must be a non-negative value.");
        }
    }

    public void setUnitTransferPrice(DataTransferPricingCategory category, double unitTransferPrice) {
        if (unitTransferPrice >= 0) {
            this.unitTransferPrice.put(category, unitTransferPrice);
        } else {
            throw new IllegalArgumentException("Unit transfer price for internet must be a non-negative value.");
        }
    }


    @Override
    public String toString() {
        return "DmlNodeUnitPrices{" +
                unitTransferPrice.toString() +
                '}';
    }
}
