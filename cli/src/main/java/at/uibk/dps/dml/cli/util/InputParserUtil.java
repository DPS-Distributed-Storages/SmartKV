package at.uibk.dps.dml.cli.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

public final class InputParserUtil {

    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private InputParserUtil() {
    }

    @SuppressWarnings({"unchecked", "java:S106"})
    public static Object[] jsonStringToObjectArray(String jsonString) throws JsonProcessingException {
        if (jsonString == null || jsonString.isEmpty()) {
            return new Object[0];
        }

        Object decodedArg = decodeJsonString(jsonString);

        Object[] args = null;
        if (decodedArg != null) {
            if (decodedArg instanceof List) {
                args = ((List<Object>) decodedArg).toArray();
            } else {
                args = new Object[]{decodedArg};
            }
        }
        return args;
    }

    private static Object decodeJsonString(String jsonString) throws JsonProcessingException {
        return jsonMapper.readValue(jsonString, Object.class);
    }
}
