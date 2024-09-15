package at.uibk.dps.dml.node.membership;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;

import static org.junit.jupiter.api.Assertions.*;

public class DmlNodeUnitPricesTest {
        private DmlNodeUnitPrices unitPrices;

        @BeforeEach
        void setUp() {
            unitPrices = new DmlNodeUnitPrices();
        }

        @Test
        void testDefaultConstructor() {
            assertEquals(0.0, unitPrices.getUnitStoragePrice());
            assertEquals(0.0, unitPrices.getUnitRequestPricePUT());
            assertEquals(0.0, unitPrices.getUnitRequestPriceGET());
            assertEquals(0.0, unitPrices.getUnitActiveKVPrice());

            DmlNodeUnitTransferPrices egressTransferPrices = unitPrices.getUnitEgressTransferPrices();
            assertNotNull(egressTransferPrices);
            assertEquals(0.0, egressTransferPrices.getUnitTransferPriceSameRegion());
            assertEquals(0.0, egressTransferPrices.getUnitTransferPriceSameProvider());
            assertEquals(0.0, egressTransferPrices.getUnitTransferPriceInternet());

            DmlNodeUnitTransferPrices ingressTransferPrices = unitPrices.getUnitIngressTransferPrices();
            assertNotNull(ingressTransferPrices);
            assertEquals(0.0, ingressTransferPrices.getUnitTransferPriceSameRegion());
            assertEquals(0.0, ingressTransferPrices.getUnitTransferPriceSameProvider());
            assertEquals(0.0, ingressTransferPrices.getUnitTransferPriceInternet());
        }

        @Test
        void testConstructorWithValidValues() {
            EnumMap<DataTransferPricingCategory, Double> transferPrices = new EnumMap<>(DataTransferPricingCategory.class);
            transferPrices.put(DataTransferPricingCategory.SAME_REGION, 5.0);
            transferPrices.put(DataTransferPricingCategory.SAME_PROVIDER, 10.0);
            transferPrices.put(DataTransferPricingCategory.INTERNET, 15.0);

            DmlNodeUnitTransferPrices unitTransferPrices = new DmlNodeUnitTransferPrices(transferPrices);

            assertDoesNotThrow(() ->
                    new DmlNodeUnitPrices(1.0, 2.0, 3.0, 4.0, unitTransferPrices, unitTransferPrices));
        }

        @Test
        void testConstructorWithNegativePrices() {
            assertThrows(IllegalArgumentException.class,
                    () -> new DmlNodeUnitPrices(-1.0, 2.0, 3.0, 4.0, new DmlNodeUnitTransferPrices(), new DmlNodeUnitTransferPrices()));
            assertThrows(IllegalArgumentException.class,
                    () -> new DmlNodeUnitPrices(1.0, -2.0, 3.0, 4.0, new DmlNodeUnitTransferPrices(), new DmlNodeUnitTransferPrices()));
            assertThrows(IllegalArgumentException.class,
                    () -> new DmlNodeUnitPrices(1.0, 2.0, -3.0, 4.0, new DmlNodeUnitTransferPrices(), new DmlNodeUnitTransferPrices()));
            assertThrows(IllegalArgumentException.class,
                    () -> new DmlNodeUnitPrices(1.0, 2.0, 3.0, -4.0, new DmlNodeUnitTransferPrices(), new DmlNodeUnitTransferPrices()));
        }

        @Test
        void testSettersWithValidValues() {
            unitPrices.setUnitStoragePrice(5.0);
            assertEquals(5.0, unitPrices.getUnitStoragePrice());

            unitPrices.setUnitRequestPricePUT(10.0);
            assertEquals(10.0, unitPrices.getUnitRequestPricePUT());

            unitPrices.setUnitRequestPriceGET(15.0);
            assertEquals(15.0, unitPrices.getUnitRequestPriceGET());

            unitPrices.setUnitActiveKVPrice(20.0);
            assertEquals(20.0, unitPrices.getUnitActiveKVPrice());

            DmlNodeUnitTransferPrices egressTransferPrices = new DmlNodeUnitTransferPrices();
            unitPrices.setUnitEgressTransferPrices(egressTransferPrices);
            assertEquals(egressTransferPrices, unitPrices.getUnitEgressTransferPrices());

            DmlNodeUnitTransferPrices ingressTransferPrices = new DmlNodeUnitTransferPrices();
            unitPrices.setUnitIngressTransferPrices(ingressTransferPrices);
            assertEquals(ingressTransferPrices, unitPrices.getUnitIngressTransferPrices());
        }

        @Test
        void testSettersWithNegativeValues() {
            assertThrows(IllegalArgumentException.class, () -> unitPrices.setUnitStoragePrice(-5.0));
            assertThrows(IllegalArgumentException.class, () -> unitPrices.setUnitRequestPricePUT(-10.0));
            assertThrows(IllegalArgumentException.class, () -> unitPrices.setUnitRequestPriceGET(-15.0));
            assertThrows(IllegalArgumentException.class, () -> unitPrices.setUnitActiveKVPrice(-20.0));

            DmlNodeUnitTransferPrices transferPrices = new DmlNodeUnitTransferPrices();
            EnumMap<DataTransferPricingCategory, Double> negativeTransferPrice = new EnumMap<>(DataTransferPricingCategory.class);
            negativeTransferPrice.put(DataTransferPricingCategory.SAME_REGION, -5.0);
            assertThrows(IllegalArgumentException.class, () -> transferPrices.setUnitTransferPrice(negativeTransferPrice));
        }

    }
