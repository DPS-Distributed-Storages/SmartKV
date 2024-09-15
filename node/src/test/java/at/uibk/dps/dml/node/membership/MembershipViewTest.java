package at.uibk.dps.dml.node.membership;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MembershipViewTest {

    @Test
    void testFindVerticleById() {
        DmlNodeInfo node1 = new DmlNodeInfo("region", "zone", "provider", "hostname", 1, false, new DmlNodeUnitPrices(), null);
        VerticleInfo verticle1 = new VerticleInfo(1, VerticleType.METADATA, 9000, node1);
        VerticleInfo verticle2 = new VerticleInfo(2, VerticleType.STORAGE, 9001, node1);
        VerticleInfo verticle3 = new VerticleInfo(3, VerticleType.STORAGE, 9002, node1);
        node1.setVerticles(Set.of(verticle1, verticle2, verticle3));
        DmlNodeInfo node2 = new DmlNodeInfo("region", "zone", "provider", "hostname", 1, false, new DmlNodeUnitPrices(),null);
        VerticleInfo verticle4 = new VerticleInfo(4, VerticleType.STORAGE, 9000, node2);
        node2.setVerticles(Set.of(verticle4));
        MembershipView view = new MembershipView(1, Map.of("node1", node1, "node2", node2));

        assertEquals(verticle1, view.findVerticleById(1));
        assertEquals(verticle2, view.findVerticleById(2));
        assertEquals(verticle3, view.findVerticleById(3));
        assertEquals(verticle4, view.findVerticleById(4));
    }

    @Test
    void testGetVerticleIdsByType() {
        DmlNodeInfo node1 = new DmlNodeInfo("region", "zone", "provider", "hostname", 1, false, new DmlNodeUnitPrices(), null);
        VerticleInfo verticle1 = new VerticleInfo(1, VerticleType.METADATA, 9000, node1);
        VerticleInfo verticle2 = new VerticleInfo(2, VerticleType.STORAGE, 9001, node1);
        VerticleInfo verticle3 = new VerticleInfo(3, VerticleType.STORAGE, 9002, node1);
        node1.setVerticles(Set.of(verticle1, verticle2, verticle3));
        DmlNodeInfo node2 = new DmlNodeInfo("region", "zone", "provider", "hostname", 1, false, new DmlNodeUnitPrices(), null);
        VerticleInfo verticle4 = new VerticleInfo(4, VerticleType.STORAGE, 9000, node2);
        node2.setVerticles(Set.of(verticle4));
        MembershipView view = new MembershipView(1, Map.of("node1", node1, "node2", node2));

        assertEquals(Collections.singleton(verticle1.getId()), view.getVerticleIdsByType(VerticleType.METADATA));
        assertEquals(Set.of(verticle2.getId(), verticle3.getId(), verticle4.getId()),
                view.getVerticleIdsByType(VerticleType.STORAGE));
    }

    @Test
    void testGetStorageVerticleIdsByZone() {
        DmlNodeInfo node1 = new DmlNodeInfo("region1", "zone1", "provider1", "hostname1", 1, false, new DmlNodeUnitPrices(), null);
        VerticleInfo verticle1 = new VerticleInfo(1, VerticleType.METADATA, 9000, node1);
        VerticleInfo verticle2 = new VerticleInfo(2, VerticleType.STORAGE, 9001, node1);
        VerticleInfo verticle3 = new VerticleInfo(3, VerticleType.STORAGE, 9002, node1);
        node1.setVerticles(Set.of(verticle1, verticle2, verticle3));
        DmlNodeInfo node2 = new DmlNodeInfo("region1", "zone1", "provider1", "hostname2", 1, false, new DmlNodeUnitPrices(), null);
        VerticleInfo verticle4 = new VerticleInfo(4, VerticleType.STORAGE, 9000, node2);
        node2.setVerticles(Set.of(verticle4));
        DmlNodeInfo node3 = new DmlNodeInfo("region3", "zone3", "provider3", "hostname3", 1, false, new DmlNodeUnitPrices(), null);
        VerticleInfo verticle5 = new VerticleInfo(5, VerticleType.METADATA, 9000, node3);
        VerticleInfo verticle6 = new VerticleInfo(6, VerticleType.STORAGE, 9001, node3);
        VerticleInfo verticle7 = new VerticleInfo(7, VerticleType.STORAGE, 9002, node3);
        node3.setVerticles(Set.of(verticle5, verticle6, verticle7));
        MembershipView view = new MembershipView(1, Map.of("node1", node1, "node2", node2, "node3", node3));

        assertEquals(Map.of("zone1", Set.of(verticle2.getId(), verticle3.getId(), verticle4.getId()),
                            "zone3", Set.of(verticle6.getId(), verticle7.getId())),
                view.getStorageVerticleIdsByZone());
    }

}
