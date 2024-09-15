package at.uibk.dps.dml.client.storage.object;

import at.uibk.dps.dml.client.DmlClient;
import at.uibk.dps.dml.client.TestHelper;
import at.uibk.dps.dml.client.metadata.MetadataCommandError;
import at.uibk.dps.dml.client.metadata.MetadataCommandErrorType;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
class SharedObjectTest {

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
    void testLockUnlock(VertxTestContext testContext) {
        String key = "sharedObject";

        client.createSharedBuffer(key)
                .onSuccess(sharedBuffer -> testContext.verify(() -> assertNull(sharedBuffer.getLockToken())))
                .compose(sharedBuffer -> sharedBuffer.lock().map(sharedBuffer))
                .onSuccess(sharedBuffer -> testContext.verify(() -> assertNotNull(sharedBuffer.getLockToken())))
                .compose(sharedBuffer -> sharedBuffer.get().map(sharedBuffer))
                .compose(sharedBuffer -> sharedBuffer.unlock().map(sharedBuffer))
                .onComplete(testContext.succeeding(sharedBuffer -> testContext.verify(() -> {
                    assertNull(sharedBuffer.getLockToken());
                    testContext.completeNow();
                })));
    }

    @Test
    void testGetFailsAfterDelete(VertxTestContext testContext) {
        String key = "sharedObject2";

        client.createSharedBuffer(key)
                .compose(sharedBuffer -> sharedBuffer.delete().map(sharedBuffer))
                .compose(SharedBuffer::get)
                .onComplete(testContext.failing(throwable -> {
                    if (throwable instanceof MetadataCommandError) {
                        MetadataCommandError error = (MetadataCommandError) throwable;
                        if (error.getErrorType() == MetadataCommandErrorType.KEY_DOES_NOT_EXIST) {
                            testContext.completeNow();
                        }
                    } else {
                        testContext.failNow("Wrong exception or error type");
                    }
                }));
    }
}
