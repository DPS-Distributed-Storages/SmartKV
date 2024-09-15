package at.uibk.dps.dml.node.metadata;

import at.uibk.dps.dml.node.membership.DmlNodeInfo;
import at.uibk.dps.dml.node.membership.DmlNodeUnitPrices;
import at.uibk.dps.dml.node.membership.VerticleInfo;
import at.uibk.dps.dml.node.membership.VerticleType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NodeAwareRandomStorageMapperTest {

    @Mock
    Random random;

    @Test
    void testSelectThrowsExceptionIfNumberOfReplicasIsLessThanOne() {
        StorageMapper storageMapper = new NodeAwareRandomStorageMapper();

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
        DmlNodeInfo nodeInfo1 = new DmlNodeInfo("region", "zone", "provider", "host1", 1024, numReplicas, false, new DmlNodeUnitPrices(),null);
        VerticleInfo verticleInfo1 = new VerticleInfo(1, VerticleType.STORAGE, 123, nodeInfo1);
        nodeInfo1.setVerticles(Set.of(verticleInfo1));
        DmlNodeInfo nodeInfo2 = new DmlNodeInfo("region", "zone", "provider", "host2", 1024, numReplicas, false, new DmlNodeUnitPrices(), null);
        VerticleInfo verticleInfo2 = new VerticleInfo(2, VerticleType.STORAGE, 123, nodeInfo2);
        nodeInfo2.setVerticles(Set.of(verticleInfo2));
        Collection<DmlNodeInfo> availableNodes = List.of(nodeInfo1, nodeInfo2);

        Set<Integer> selected = storageMapper.select(availableNodes, numReplicas);

        assertEquals(numReplicas, selected.size());
        assertTrue(selected.contains(verticleInfo1.getId()) || selected.contains(verticleInfo2.getId()));
    }

    @Test
    void testSelectTwoReplicas() {
        int numReplicas = 2;
        when(random.nextInt(anyInt())).thenReturn(0);
        StorageMapper storageMapper = new RandomStorageMapper(random);
        DmlNodeInfo nodeInfo1 = new DmlNodeInfo("region", "zone", "provider", "host1", 1024, numReplicas, false, new DmlNodeUnitPrices(), null);
        VerticleInfo verticleInfo1 = new VerticleInfo(1, VerticleType.STORAGE, 123, nodeInfo1);
        nodeInfo1.setVerticles(Set.of(verticleInfo1));
        DmlNodeInfo nodeInfo2 = new DmlNodeInfo("region", "zone", "provider", "host2", 1024, numReplicas, false, new DmlNodeUnitPrices(), null);
        VerticleInfo verticleInfo2 = new VerticleInfo(2, VerticleType.STORAGE, 123, nodeInfo2);
        nodeInfo2.setVerticles(Set.of(verticleInfo2));
        Collection<DmlNodeInfo> availableNodes = List.of(nodeInfo1, nodeInfo2);

        Set<Integer> selected = storageMapper.select(availableNodes, numReplicas);

        assertEquals(numReplicas, selected.size());
        assertTrue(selected.contains(verticleInfo1.getId()));
        assertTrue(selected.contains(verticleInfo2.getId()));
    }
}
