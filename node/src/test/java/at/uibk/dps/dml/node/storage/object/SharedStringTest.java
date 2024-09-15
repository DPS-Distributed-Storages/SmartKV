package at.uibk.dps.dml.node.storage.object;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SharedStringTest {

    @Test
    void testEmptyConstructor() {
        SharedString string = new SharedString();
        assertNull(string.get());
    }

    @Test
    void testConstructorWithInitialValue() {
        String initial = "initial string";

        SharedString buffer = new SharedString(initial);

        assertEquals(initial, buffer.get());
    }

    @Test
    void testSetGet() {
        String value = "value";
        SharedString string = new SharedString();

        string.set(value);

        assertEquals(value, string.get());
    }
}
