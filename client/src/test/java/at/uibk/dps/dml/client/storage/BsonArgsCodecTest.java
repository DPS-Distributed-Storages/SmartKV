package at.uibk.dps.dml.client.storage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class BsonArgsCodecTest {

    @Test
    void testEncodeDecode() {
        BsonArgsCodec tested = new BsonArgsCodec();
        Object[] value = new Object[] { 1,2,3 };

        byte[] encoded = tested.encode(value);
        Object[] decoded = tested.decode(encoded);

        assertArrayEquals(value, decoded);
    }
}
