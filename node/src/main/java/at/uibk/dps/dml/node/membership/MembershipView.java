package at.uibk.dps.dml.node.membership;

import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MembershipView {

    private final int epoch;

    private final Map<String, DmlNodeInfo> nodeMap;

    public MembershipView(int epoch, Map<String, DmlNodeInfo> nodeMap) {
        this.epoch = epoch;
        this.nodeMap = nodeMap;
    }

    public int getEpoch() {
        return epoch;
    }

    public Map<String, DmlNodeInfo> getNodeMap() {
        return nodeMap;
    }

    public VerticleInfo findVerticleById(int verticleId) {
        return nodeMap.values().stream()
                .flatMap(node -> node.getVerticles().stream())
                .filter(verticleInfo -> verticleInfo.getId() == verticleId)
                .findFirst().orElse(null);
    }

    public Set<Integer> getVerticleIdsByType(VerticleType type) {
        return nodeMap.values().stream()
                .flatMap(node -> node.getVerticles().stream())
                .filter(verticle -> verticle.getType() == type)
                .map(VerticleInfo::getId)
                .collect(Collectors.toSet());
    }

    public Map<String, Set<Integer>> getStorageVerticleIdsByZone() {
        HashMap<String, Set<Integer>> storageVerticleIdsByZone = new HashMap<>();
        for(DmlNodeInfo node : nodeMap.values()){
            if(!storageVerticleIdsByZone.containsKey(node.getZone())) {
                storageVerticleIdsByZone.put(node.getZone(), node.getVerticles().stream().filter(verticle -> verticle.getType() == VerticleType.STORAGE).map(VerticleInfo::getId).collect(Collectors.toSet()));
            }
            else{
                storageVerticleIdsByZone.get(node.getZone()).addAll(node.getVerticles().stream().filter(verticle -> verticle.getType() == VerticleType.STORAGE).map(VerticleInfo::getId).collect(Collectors.toSet()));
            }
        }
        return storageVerticleIdsByZone;
    }

    public Map<String, Set<Integer>> getMetadataVerticleIdsByZone() {
        HashMap<String, Set<Integer>> metadataVerticleIdsByZone = new HashMap<>();
        for(DmlNodeInfo node : nodeMap.values()){
            if(!metadataVerticleIdsByZone.containsKey(node.getZone())) {
                metadataVerticleIdsByZone.put(node.getZone(), node.getVerticles().stream().filter(verticle -> verticle.getType() == VerticleType.METADATA).map(VerticleInfo::getId).collect(Collectors.toSet()));
            }
            else{
                metadataVerticleIdsByZone.get(node.getZone()).addAll(node.getVerticles().stream().filter(verticle -> verticle.getType() == VerticleType.METADATA).map(VerticleInfo::getId).collect(Collectors.toSet()));
            }
        }
        return metadataVerticleIdsByZone;
    }

    @Override
    public String toString() {
        return "MembershipView{" +
                "epoch=" + epoch +
                ", nodeMap=" + JsonObject.mapFrom(nodeMap).encodePrettily() +
                '}';
    }
}
