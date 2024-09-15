package at.uibk.dps.dml.client.storage.object;

import at.uibk.dps.dml.client.DmlClient;
import at.uibk.dps.dml.client.NodeLocation;
import at.uibk.dps.dml.client.TestConfig;
import at.uibk.dps.dml.client.TestHelper;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Array;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
class SharedStatisticsTest {

    private DmlClient client;

    @BeforeAll
    static void beforeAll(Vertx vertx, VertxTestContext testContext){

        DmlClient client = new DmlClient(vertx, new NodeLocation("localRegion", "ap1", "localProvider"));



        final String EXTENSION_PACKAGE = "at.uibk.dps.dml.client.storage.object.extensions.";
        final String EXTENSION_CLASS = "SharedStatistics";
        Class<?> clazz;

        try {
            clazz = Class.forName(EXTENSION_PACKAGE + EXTENSION_CLASS);
        } catch (ClassNotFoundException e) {
            System.err.println("Could not find the class file: " + e.getMessage());
            return;
        }

        byte[] byteCode = TestHelper.readBytesFromClass(clazz);

        client.connect(TestConfig.METADATA_HOST, TestConfig.METADATA_PORT).onComplete(res ->
                {
        client.registerSharedJavaClass(EXTENSION_CLASS, byteCode)
                .compose(v -> client.getAllConfigurations())
                .onComplete(testContext.succeeding(configs -> testContext.verify(() -> {
                    assertTrue(configs.containsKey(EXTENSION_CLASS));
                    assertFalse(configs.get(EXTENSION_CLASS).getReplicas().isEmpty());
                    testContext.completeNow();
                })));});

        client.disconnect();

    }

    @BeforeEach
    void beforeEach(Vertx vertx, VertxTestContext testContext) {
        client = TestHelper.createDmlClient(vertx, testContext);
    }

    @AfterEach
    void afterEach(VertxTestContext testContext) {
        TestHelper.deleteAllKeys(client)
                .compose(v -> client.disconnect())
                .onComplete(testContext.succeedingThenComplete());
    }

    @Test
    void testGet(VertxTestContext testContext) {
        String key = "statistics1";

        client.create(key, "SharedStatistics")
                .map(res -> new SharedStatistics(client, key))
                .compose(SharedStatistics::get)
                .onComplete(testContext.succeeding(value -> testContext.verify(() -> {
                    assertEquals(0, value.size());
                    testContext.completeNow();
                })));
    }


    @Test
    void testAddGet(VertxTestContext testContext) {
        String key = "statistics2";

        client.create(key, "SharedStatistics")
                .map(res -> new SharedStatistics(client, key))
                .compose(sharedStatistics -> sharedStatistics.add(5L).map(sharedStatistics))
                .compose(sharedStatistics -> sharedStatistics.add(10L).map(sharedStatistics))
                .compose(SharedStatistics::get)
                .onComplete(testContext.succeeding(value -> testContext.verify(() -> {
                    assertEquals(2, value.size());
                    assertEquals(5L, value.get(0));
                    assertEquals(10L, value.get(1));
                    testContext.completeNow();
                })));
    }

    @Test
    void testAddRemove(VertxTestContext testContext) {
        String key = "statistics3";

        client.create(key, "SharedStatistics")
                .map(res -> new SharedStatistics(client, key))
                .compose(sharedStatistics -> sharedStatistics.add(5L).map(sharedStatistics))
                .compose(sharedStatistics -> sharedStatistics.add(10L).map(sharedStatistics))
                .compose(sharedStatistics -> sharedStatistics.remove(10L).map(sharedStatistics))
                .compose(SharedStatistics::get)
                .onComplete(testContext.succeeding(value -> testContext.verify(() -> {
                    assertEquals(1, value.size());
                    assertEquals(5L, value.get(0));
                    assertThrows(IndexOutOfBoundsException.class, () -> value.get(1));
                    testContext.completeNow();
                })));
    }

    @Test
    void testAddRemoveAverage(VertxTestContext testContext) {
        String key = "statistics4";

        client.create(key, "SharedStatistics")
                .map(res -> new SharedStatistics(client, key))
                .compose(sharedStatistics -> sharedStatistics.add(5L).map(sharedStatistics))
                .compose(sharedStatistics -> sharedStatistics.add(10L).map(sharedStatistics))
                .compose(sharedStatistics -> sharedStatistics.remove(10L).map(sharedStatistics))
                .compose(sharedStatistics -> sharedStatistics.add(15L).map(sharedStatistics))
                .compose(SharedStatistics::average)
                .onComplete(testContext.succeeding(value -> testContext.verify(() -> {
                    assertEquals((5 + 15) / 2.0, value);
                    testContext.completeNow();
                })));
    }

}
