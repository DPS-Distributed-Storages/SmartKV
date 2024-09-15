package at.uibk.dps.dml.client.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.undercouch.bson4jackson.BsonFactory;

import java.io.IOException;

/**
 * Encodes and decodes the arguments of a shared object using BSON.
 */
public class BsonArgsCodec implements SharedObjectArgsCodec {

    // Creating object mappers is expensive, so we use a thread-local variable to store them. Thread local is used
    // because sharing the object mapper between threads can lead to performance problems due to synchronization.
    // https://stackoverflow.com/a/52584949
    private static final ThreadLocal<ObjectMapper> objectMapper = ThreadLocal.withInitial(() -> new ObjectMapper(new BsonFactory()));

    @Override
    public byte[] encode(Object[] args) {
        try {
            return objectMapper.get().writeValueAsBytes(args);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Object[] decode(byte[] encodedArgs) {
        if (encodedArgs == null) {
            return new Object[0];
        }
        try {
            return objectMapper.get().readValue(encodedArgs, Object[].class);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Releases thread local variables.
     */
    public void unload() {
        objectMapper.remove();
    }
}
