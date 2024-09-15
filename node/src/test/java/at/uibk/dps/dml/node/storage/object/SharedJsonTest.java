package at.uibk.dps.dml.node.storage.object;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SharedJsonTest {

    private Map<String, Object> testData;

    @BeforeEach
    void beforeEach() throws JsonProcessingException {
        testData = new ObjectMapper().readValue(testJsonString, new TypeReference<>() {});
    }

    @Test
    void testCreateEmpty() {
        SharedJson json = new SharedJson();

        assertEquals(Collections.emptyMap(), json.getJson());
    }

    @Test
    void testCreateWithInitialValue() {
        SharedJson json = new SharedJson(testData);

        assertEquals(testData, json.getJson());
    }

    @Test
    void testSetRoot() {
        SharedJson json = new SharedJson(testData);
        int updatedRoot = 123;

        json.set("$", updatedRoot);

        assertEquals(updatedRoot, json.getJson());
    }

    @Test
    void testSetGetPath() {
        Map<String, Integer> initial = new HashMap<>(Collections.singletonMap("key", 123));
        SharedJson json = new SharedJson(initial);

        json.set("$.key", 5);

        assertEquals(5, json.get("$.key"));
    }

    @Test
    void testGetWithInvalidPathFails() {
        SharedJson json = new SharedJson(testData);

        assertThrows(PathNotFoundException.class, () -> json.get("$.invalid.path"));
    }

    @Test
    void testPutGetPath() {
        SharedJson json = new SharedJson(testData);

        json.put("$.store", "newKey", "newValue");

        assertEquals("newValue", json.get("$.store.newKey"));
    }

    @Test
    void testAdd() {
        SharedJson json = new SharedJson(testData);

        json.add("$.store.book", 25);

        assertEquals(25, json.get("$.store.book[4]"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDeletePath() {
        SharedJson json = new SharedJson(testData);

        json.delete("$.store.book[0]");

        assertEquals(3, ((ArrayList<Map<String, Object>>) json.get("$.store.book")).size());
    }

    @Test
    void testDeleteRoot() {
        SharedJson json = new SharedJson(testData);

        json.delete("$");

        assertEquals(new SharedJson().getJson(), json.getJson());
    }

    @Test
    void testSerializationDeserialization() {
        SharedJson original = new SharedJson(testData);

        SharedJson copy = SerializationUtils.clone(original);

        assertEquals(original.getJson(), copy.getJson());
    }

    @Test
    void testGetAsString() throws JsonProcessingException {
        SharedJson json = new SharedJson(testData);

        String jsonString = json.getAsString();

        assertEquals(testData, new ObjectMapper().readValue(jsonString, new TypeReference<>() {}));
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
