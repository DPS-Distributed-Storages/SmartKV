package at.uibk.dps.dml.node.util;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TimestampTest {

    @Test
    void testConstructor() {
        int version = 2;
        int coordinatorVerticleId = 3;

        Timestamp timestamp = new Timestamp(version, coordinatorVerticleId);

        assertEquals(version, timestamp.getVersion());
        assertEquals(coordinatorVerticleId, timestamp.getCoordinatorVerticleId());
    }

    @Test
    void testCopyConstructor() {
        Timestamp timestamp1 = new Timestamp(2, 3);
        Timestamp timestamp2 = new Timestamp(timestamp1);

        assertEquals(timestamp1.getVersion(), timestamp2.getVersion());
        assertEquals(timestamp1.getCoordinatorVerticleId(), timestamp2.getCoordinatorVerticleId());
    }

    @ParameterizedTest
    @CsvSource({
            "1, 1, 2, 1, true",
            "1, 1, 1, 2, true",
            "1, 1, 2, 2, true",
            "1, 1, 1, 1, false",
            "2, 1, 1, 1, false",
            "1, 2, 1, 1, false",
            "2, 1, 1, 2, false",
    })
    void testIsLessThan(int version1, int coordinatorVerticleId1, int version2, int coordinatorVerticleId2, boolean expected) {
        Timestamp timestamp1 = new Timestamp(version1, coordinatorVerticleId1);
        Timestamp timestamp2 = new Timestamp(version2, coordinatorVerticleId2);
        assertEquals(expected, timestamp1.isLessThan(timestamp2));
    }

    @ParameterizedTest
    @CsvSource({
            "2, 1, 1, 1, true",
            "1, 2, 1, 1, true",
            "2, 1, 1, 2, true",
            "1, 1, 1, 1, false",
            "1, 1, 2, 1, false",
            "1, 1, 1, 2, false",
            "1, 1, 2, 2, false",
    })
    void testIsGreaterThan(int version1, int coordinatorVerticleId1, int version2, int coordinatorVerticleId2, boolean expected) {
        Timestamp timestamp1 = new Timestamp(version1, coordinatorVerticleId1);
        Timestamp timestamp2 = new Timestamp(version2, coordinatorVerticleId2);
        assertEquals(expected, timestamp1.isGreaterThan(timestamp2));
    }

    @ParameterizedTest
    @CsvSource({
            "1, 1, 1, 1, 0",
            "1, 1, 2, 1, -1",
            "1, 1, 1, 2, -1",
            "1, 1, 2, 2, -1",
            "2, 1, 1, 1, 1",
            "1, 2, 1, 1, 1",
            "2, 1, 1, 2, 1",
    })
    void testCompareTo(int version1, int coordinatorVerticleId1, int version2, int coordinatorVerticleId2, int expected) {
        Timestamp timestamp1 = new Timestamp(version1, coordinatorVerticleId1);
        Timestamp timestamp2 = new Timestamp(version2, coordinatorVerticleId2);
        assertEquals(expected, timestamp1.compareTo(timestamp2));
    }

    @ParameterizedTest
    @CsvSource({
            "1, 1, 1, 1, true",
            "1, 1, 2, 1, false",
            "1, 1, 1, 2, false",
            "1, 1, 2, 2, false",
            "2, 1, 1, 1, false",
            "1, 2, 1, 1, false",
            "2, 1, 1, 2, false",
    })
    void testEquals(int version1, int coordinatorVerticleId1, int version2, int coordinatorVerticleId2, boolean expected) {
        Timestamp timestamp1 = new Timestamp(version1, coordinatorVerticleId1);
        Timestamp timestamp2 = new Timestamp(version2, coordinatorVerticleId2);
        assertEquals(expected, timestamp1.equals(timestamp2));
    }


    @Test
    void testEqualsHashCode() {
        EqualsVerifier.forClass(Timestamp.class).usingGetClass().suppress(Warning.NONFINAL_FIELDS).verify();
    }
}
