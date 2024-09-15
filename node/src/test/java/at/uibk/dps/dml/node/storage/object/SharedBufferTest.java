package at.uibk.dps.dml.node.storage.object;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SharedBufferTest {

    @Test
    void testEmptyConstructor() {
        SharedBuffer buffer = new SharedBuffer();
        assertNull(buffer.get());
    }

    @Test
    void testConstructorWithInitialValue() {
        byte[] initial = new byte[]{1, 2, 3};

        SharedBuffer buffer = new SharedBuffer(initial);

        assertEquals(initial, buffer.get());
    }

    @Test
    void testSetGet() {
        byte[] value = new byte[]{3, 2, 3};
        SharedBuffer buffer = new SharedBuffer();

        buffer.set(value);

        assertEquals(value, buffer.get());
    }
}
