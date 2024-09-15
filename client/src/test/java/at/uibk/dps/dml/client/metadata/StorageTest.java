package at.uibk.dps.dml.client.metadata;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.jupiter.api.Test;

class StorageTest {

    @Test
    void testEquals() {
        EqualsVerifier.forClass(Storage.class).usingGetClass().suppress(Warning.NONFINAL_FIELDS).verify();
    }
}
