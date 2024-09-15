package at.uibk.dps.dml.client.storage.object;

import at.uibk.dps.dml.client.DmlClient;
import at.uibk.dps.dml.client.TestHelper;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@ExtendWith(VertxExtension.class)
class SharedBufferTest {

    private DmlClient client;

    @BeforeEach
    void beforeEach(Vertx vertx, VertxTestContext testContext) {
        client = TestHelper.createDmlClient(vertx, testContext);
    }

    @AfterEach
    void afterEach(VertxTestContext testContext) {
        client.disconnect().onComplete(testContext.succeedingThenComplete());
    }

    @Test
    void testGetInitialValue(VertxTestContext testContext) {
        String key = "buffer1";
        byte[] initial = "bufferValue1".getBytes();

        client.createSharedBuffer(key, initial)
                .compose(SharedBuffer::get)
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertArrayEquals(initial, result);
                    testContext.completeNow();
                })));
    }

    @Test
    void testSetGet(VertxTestContext testContext) {
        String key = "buffer2";
        byte[] value = "bufferValue2".getBytes();

        client.createSharedBuffer(key)
                .compose(sharedBuffer -> sharedBuffer.set(value).map(sharedBuffer))
                .compose(SharedBuffer::get)
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertArrayEquals(value, result);
                    testContext.completeNow();
                })));
    }
}
