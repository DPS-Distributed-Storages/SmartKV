package at.uibk.dps.dml.client;

import at.uibk.dps.dml.client.metadata.MetadataCommandError;
import at.uibk.dps.dml.client.metadata.MetadataCommandErrorType;
import at.uibk.dps.dml.client.metadata.Storage;
import at.uibk.dps.dml.client.storage.SimpleStorageSelector;
import at.uibk.dps.dml.client.storage.StorageClient;
import at.uibk.dps.dml.client.storage.object.SharedBuffer;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
class DmlClientTest {

    private DmlClient client;

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
    void testGetMembershipView(VertxTestContext testContext) {
        client.getMembershipView()
                .onComplete(testContext.succeeding(res -> testContext.verify(() -> {
                    assertTrue(res.contains("epoch"));
                    testContext.completeNow();
                })));
    }

    @Test
    void testCreateIsIgnoredIfKeyAlreadyExists(VertxTestContext testContext) {
        String key = "key1";

        client.create(key)
                .compose(v -> client.create(key))
                .onComplete(testContext.succeedingThenComplete());
    }

    @Test
    void testCreateFailsIfKeyExistsAndIgnoreExistingFlagIsNotSet(VertxTestContext testContext) {
        String key = "key2";

        client.create(key)
                .compose(v -> client.create(key, null, false, "java", "SharedBuffer", null, false))
                .onComplete(testContext.failing(throwable -> {
                    if (throwable instanceof MetadataCommandError) {
                        MetadataCommandError error = (MetadataCommandError) throwable;
                        if (error.getErrorType() == MetadataCommandErrorType.KEY_ALREADY_EXISTS) {
                            testContext.completeNow();
                        }
                    } else {
                        testContext.failNow("Wrong exception or error type");
                    }
                }));
    }

    @Test
    void testInvokeMethod(VertxTestContext testContext) {
        String key = "key3";
        byte[] value = "myValue1".getBytes();

        client.create(key, "SharedBuffer")
                .compose(v -> client.invokeMethod(key, "set", new Object[]{value}))
                .compose(v -> client.invokeMethod(key, "get", null))
                .onComplete(testContext.succeeding(res -> testContext.verify(() -> {
                    assertArrayEquals(value, (byte[]) res);
                    testContext.completeNow();
                })));
    }

    @Test
    void testInvokeMethodMultipleKeys(VertxTestContext testContext) {
        Set<String> keys = new HashSet<>(Arrays.asList("key3_1", "key3_2", "key3_3"));
        List<Future> futures = new ArrayList<>();
        for (String key : keys) {
            Promise<Void> promise = Promise.promise();
            client.create(key, "SharedCounter")
                    .compose(v -> client.getAllConfigurations())
                    .onComplete(testContext.succeeding(configs -> testContext.verify(() -> {
                        assertTrue(configs.containsKey(key));
                        promise.complete();
                    })));
        futures.add(promise.future());
        }

        long increment = 3;

        CompositeFuture.all(futures).onComplete(r ->
                client.invokeMethod(keys, "increment", new Object[]{increment})
                .onSuccess(res -> {
                    client.invokeMethod(keys, "get", null).onSuccess(finalRes -> testContext.verify(() -> {
                                for(int i = 0; i < finalRes.size(); i++){
                                    assertEquals(increment, (long)finalRes.resultAt(i));
                                    System.out.println((long)finalRes.resultAt(i));
                                }
                                testContext.completeNow();
                            }))
                            .onFailure(finalRes -> {
                                testContext.failNow(finalRes.getMessage());
                            });
                })
                .onFailure(res -> {
                    testContext.failNow(res.getMessage());
                }));

    }

    @Test
    @SuppressWarnings("unchecked")
    void testSetGet(VertxTestContext testContext) {
        String key = "key4";
        byte[] value = "myValue2".getBytes();

        client.create(key)
                .compose(v -> client.set(key, value))
                .compose(v -> client.get(key))
                .onComplete(testContext.succeeding(res -> testContext.verify(() -> {
                    assertArrayEquals(value, (byte[]) ((Map<String, Object>) res).get("buffer"));
                    testContext.completeNow();
                })));
    }

    @Test
    void testGetFailsAfterDelete(VertxTestContext testContext) {
        String key = "key5";

        client.create(key)
                .compose(v -> client.delete(key))
                .compose(v -> client.get(key))
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

    @Test
    void testGetAllConfigurations(VertxTestContext testContext) {
        String key1 = "key1";
        String key2 = "key2";

        client.create(key1)
                .compose(v -> client.create(key2))
                .compose(v -> client.getAllConfigurations())
                .onComplete(testContext.succeeding(configs -> testContext.verify(() -> {
                    assertEquals(2, configs.size());
                    assertTrue(configs.containsKey(key1));
                    assertTrue(configs.containsKey(key2));
                    testContext.completeNow();
                })));
    }

    @Test
    void testRegisterJavaClass(VertxTestContext testContext) {

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

        client.registerSharedJavaClass(EXTENSION_CLASS, byteCode)
                .compose(v -> client.getAllConfigurations())
                .onComplete(testContext.succeeding(configs -> testContext.verify(() -> {
                    assertTrue(configs.containsKey(EXTENSION_CLASS));
                    assertFalse(configs.get(EXTENSION_CLASS).getReplicas().isEmpty());
                    testContext.completeNow();
                })));
    }

    @Test
    void testRegisterJavaClassAndUseIt(VertxTestContext testContext) {

        final String EXTENSION_PACKAGE = "at.uibk.dps.dml.client.storage.object.extensions.";
        final String EXTENSION_CLASS = "SharedStatistics";
        final String EXTENSION_LANGUAGE = "java";
        String key1 = "key_ext_1";

        Class<?> clazz;

        try {
            clazz = Class.forName(EXTENSION_PACKAGE + EXTENSION_CLASS);
        } catch (ClassNotFoundException e) {
            System.err.println("Could not find the class file: " + e.getMessage());
            return;
        }

        byte[] byteCode = TestHelper.readBytesFromClass(clazz);

        client.registerSharedJavaClass(EXTENSION_CLASS, byteCode)
                .compose(v -> client.create(key1, null, true, EXTENSION_LANGUAGE, EXTENSION_CLASS, null, false))
                .compose(v -> client.getAllConfigurations())
                .onComplete(testContext.succeeding(configs -> testContext.verify(() -> {
                    assertTrue(configs.containsKey(EXTENSION_CLASS));
                    assertTrue(configs.containsKey(key1));
                    assertFalse(configs.get(EXTENSION_CLASS).getReplicas().isEmpty());
                    testContext.completeNow();
                })));
    }

    @Test
    void testRegisterLuaClass(VertxTestContext testContext) {

        final String LUA_EXTENSION = this.getClass().getClassLoader().getResource("Rectangle.lua").getPath();
        final String EXTENSION_CLASS = "Rectangle";

        byte[] byteCode = TestHelper.readBytesFromFile(Path.of(LUA_EXTENSION));

        client.registerSharedLuaClass(EXTENSION_CLASS, byteCode)
                .compose(v -> client.getAllConfigurations())
                .onComplete(testContext.succeeding(configs -> testContext.verify(() -> {
                    assertTrue(configs.containsKey(EXTENSION_CLASS));
                    assertFalse(configs.get(EXTENSION_CLASS).getReplicas().isEmpty());
                    testContext.completeNow();
                })));
    }

    @Test
    void testRegisterLuaClassAndUseIt(VertxTestContext testContext) {

        final String LUA_EXTENSION = this.getClass().getClassLoader().getResource("Rectangle.lua").getPath();
        final String EXTENSION_CLASS = "Rectangle";
        final String EXTENSION_LANGUAGE = "lua";
        String key1 = "key_rectangle_1";

        byte[] byteCode = TestHelper.readBytesFromFile(Path.of(LUA_EXTENSION));
        int length = 10, breadth = 3;

        client.registerSharedLuaClass(EXTENSION_CLASS, byteCode)
                .compose(v -> client.create(key1, null, true, EXTENSION_LANGUAGE, EXTENSION_CLASS, new Object[]{length,breadth}, false))
                .compose(v -> client.invokeMethod(key1, "getArea", null))
                .compose(v -> client.getAllConfigurations())
                .onComplete(testContext.succeeding(configs -> testContext.verify(() -> {
                    assertTrue(configs.containsKey(EXTENSION_CLASS));
                    assertTrue(configs.containsKey(key1));
                    assertFalse(configs.get(EXTENSION_CLASS).getReplicas().isEmpty());
                    testContext.completeNow();
                })));
    }

    @Test
    void testGetZoneInfo(VertxTestContext testContext) {
        client.getZoneInfo("local_zone1")
                .onComplete(testContext.succeeding(zoneInfo -> testContext.verify(() -> {
                    assertDoesNotThrow(() -> new Exception());
                    System.out.println(zoneInfo);
                    testContext.completeNow();
                })));
    }

    @Test
    void testGetFreeStorageNode(VertxTestContext testContext) {
        LinkedHashSet<String> zones = new LinkedHashSet<>();
        zones.add("local_zone1");
        client.getFreeStorageNodes(zones, 1024)
                .onComplete(testContext.succeeding(storageNodes -> testContext.verify(() -> {
                    assertDoesNotThrow(() -> new Exception());
                    System.out.println(storageNodes);
                    testContext.completeNow();
                })));
    }

}
