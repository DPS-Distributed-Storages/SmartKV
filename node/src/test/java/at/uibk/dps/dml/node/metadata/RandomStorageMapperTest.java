package at.uibk.dps.dml.node.metadata;

import at.uibk.dps.dml.node.membership.DmlNodeInfo;
import at.uibk.dps.dml.node.membership.DmlNodeUnitPrices;
import at.uibk.dps.dml.node.membership.VerticleInfo;
import at.uibk.dps.dml.node.membership.VerticleType;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RandomStorageMapperTest {

    @Test
    void testSelectThrowsExceptionIfNumberOfReplicasIsLessThanOne() {
        StorageMapper storageMapper = new RandomStorageMapper();

        assertThrows(IllegalArgumentException.class, () -> storageMapper.select(null, 0));
        assertThrows(IllegalArgumentException.class, () -> storageMapper.select(null, -1));
    }

    @Test
    void testSelectThrowsExceptionIfAvailableNodesIsEmpty() {
        StorageMapper storageMapper = new RandomStorageMapper();
        Collection<DmlNodeInfo> availableNodes = Collections.emptyList();

        assertThrows(IllegalArgumentException.class, () -> storageMapper.select(availableNodes, 1));
    }

    @Test
    void testSelectSingleReplica() {
        int numReplicas = 1;
        StorageMapper storageMapper = new RandomStorageMapper(new Random());
        DmlNodeInfo node = new DmlNodeInfo("region", "zone", "provider", "hostname", 1024, numReplicas, true, new DmlNodeUnitPrices(), null);
        VerticleInfo verticle1 = new VerticleInfo(1, VerticleType.STORAGE, 123, node);
        VerticleInfo verticle2 = new VerticleInfo(2, VerticleType.STORAGE, 124, node);
        node.setVerticles(Set.of(verticle1, verticle2));
        Collection<DmlNodeInfo> availableNodes = Collections.singleton(node);

        Set<Integer> selected = storageMapper.select(availableNodes, numReplicas);

        assertEquals(numReplicas, selected.size());
        assertTrue(selected.contains(verticle1.getId()) || selected.contains(verticle2.getId()));
    }

    @Test
    void testSelectMultipleReplicas() {
        int numReplicas = 2;
        StorageMapper storageMapper = new RandomStorageMapper(new Random());
        DmlNodeInfo node = new DmlNodeInfo("region", "zone", "provider", "hostname", 1024, numReplicas, true, new DmlNodeUnitPrices(), null);
        VerticleInfo verticle1 = new VerticleInfo(1, VerticleType.STORAGE, 123, node);
        VerticleInfo verticle2 = new VerticleInfo(2, VerticleType.STORAGE, 124, node);
        node.setVerticles(Set.of(verticle1, verticle2));
        Collection<DmlNodeInfo> availableNodes = Collections.singleton(node);

        Set<Integer> selected = storageMapper.select(availableNodes, numReplicas);

        assertEquals(numReplicas, selected.size());
        assertTrue(selected.contains(verticle1.getId()));
        assertTrue(selected.contains(verticle2.getId()));
    }
}
