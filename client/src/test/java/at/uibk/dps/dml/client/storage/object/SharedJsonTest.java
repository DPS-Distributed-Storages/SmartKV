package at.uibk.dps.dml.client.storage.object;

import at.uibk.dps.dml.client.DmlClient;
import at.uibk.dps.dml.client.TestHelper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(VertxExtension.class)
class SharedJsonTest {

    private static Map<String, Object> testData;

    private DmlClient client;

    @BeforeAll
    static void beforeAll() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        testData = objectMapper.readValue(testJsonString, new TypeReference<>() {});
    }

    @BeforeEach
    void beforeEach(Vertx vertx, VertxTestContext testContext) {
        client = TestHelper.createDmlClient(vertx, testContext);
    }

    @AfterEach
    void afterEach(VertxTestContext testContext) {
        client.disconnect().onComplete(testContext.succeedingThenComplete());
    }

    @Test
    void testCreate(VertxTestContext testContext) {
        client.createSharedJson("testCreate")
                .compose(SharedJson::get)
                .onComplete(testContext.succeeding(value -> testContext.verify(() -> {
                    assertEquals(Collections.emptyMap(), value);
                    testContext.completeNow();
                })));
    }

    @Test
    void testCreateWithInitialValue(VertxTestContext testContext) {
        client.createSharedJson("testCreateWithInitialValue", testData)
                .compose(SharedJson::get)
                .onComplete(testContext.succeeding(value -> testContext.verify(() -> {
                    assertEquals(testData, value);
                    testContext.completeNow();
                })));
    }

    @Test
    void testSetGet(VertxTestContext testContext) {
        client.createSharedJson("testSetGet")
                .compose(sharedJson -> sharedJson.set(testData).map(sharedJson))
                .compose(SharedJson::get)
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertEquals(testData, result);
                    testContext.completeNow();
                })));
    }

    @Test
    void testSetGetPath(VertxTestContext testContext) {
        client.createSharedJson("testSetGetPath", testData)
                .compose(sharedJson -> sharedJson.set("$.store.bicycle.price", 20.0).map(sharedJson))
                .compose(sharedJson -> sharedJson.get("$.store.bicycle.price"))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertEquals(20.0, result);
                    testContext.completeNow();
                })));
    }

    @Test
    void testGetPath(VertxTestContext testContext) {
        client.createSharedJson("testGetPath", testData)
                .compose(sharedJson -> sharedJson.get("$.store.book[0].price"))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertEquals(8.95, result);
                    testContext.completeNow();
                })));
    }

    @Test
    void testGetWithInvalidPathFails(VertxTestContext testContext) {
        client.createSharedJson("testGetWithInvalidPathFails", testData)
                .compose(sharedJson -> sharedJson.get("$.non.existing.path"))
                .onComplete(testContext.failingThenComplete());
    }

    @Test
    void testPutGetRootPath(VertxTestContext testContext) {
        client.createSharedJson("testPutGetRootPath")
                .compose(sharedJson -> sharedJson.put("test", 123).map(sharedJson))
                .compose(sharedJson -> sharedJson.get("$.test"))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertEquals(123, result);
                    testContext.completeNow();
                })));
    }

    @Test
    void testPutGetPath(VertxTestContext testContext) {
        client.createSharedJson("testPutGetPath", testData)
                .compose(sharedJson -> sharedJson.put("$.store", "test", 321).map(sharedJson))
                .compose(sharedJson -> sharedJson.get("$.store.test"))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertEquals(321, result);
                    testContext.completeNow();
                })));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDeletePath(VertxTestContext testContext) {
        client.createSharedJson("testDeletePath", testData)
                .compose(sharedJson -> sharedJson.delete("$.store.book[0]").map(sharedJson))
                .compose(sharedJson -> sharedJson.get("$.store.book"))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertEquals(3, ((ArrayList<Map<String, Object>>) result).size());
                    testContext.completeNow();
                })));
    }

    // taken from https://github.com/json-path/JsonPath
    private static final String testJsonString =
            "{\n" +
                    "    \"store\": {\n" +
                    "        \"book\": [\n" +
                    "            {\n" +
                    "                \"category\": \"reference\",\n" +
                    "                \"author\": \"Nigel Rees\",\n" +
                    "                \"title\": \"Sayings of the Century\",\n" +
                    "                \"price\": 8.95\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"category\": \"fiction\",\n" +
                    "                \"author\": \"Evelyn Waugh\",\n" +
                    "                \"title\": \"Sword of Honour\",\n" +
                    "                \"price\": 12.99\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"category\": \"fiction\",\n" +
                    "                \"author\": \"Herman Melville\",\n" +
                    "                \"title\": \"Moby Dick\",\n" +
                    "                \"isbn\": \"0-553-21311-3\",\n" +
                    "                \"price\": 8.99\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"category\": \"fiction\",\n" +
                    "                \"author\": \"J. R. R. Tolkien\",\n" +
                    "                \"title\": \"The Lord of the Rings\",\n" +
                    "                \"isbn\": \"0-395-19395-8\",\n" +
                    "                \"price\": 22.99\n" +
                    "            }\n" +
                    "        ],\n" +
                    "        \"bicycle\": {\n" +
                    "            \"color\": \"red\",\n" +
                    "            \"price\": 19.95\n" +
                    "        }\n" +
                    "    },\n" +
                    "    \"expensive\": 10\n" +
                    "}";
}
