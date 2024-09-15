package at.uibk.dps.dml.node.metadata;

import at.uibk.dps.dml.node.membership.DmlNodeInfo;
import at.uibk.dps.dml.node.membership.VerticleInfo;
import at.uibk.dps.dml.node.membership.VerticleType;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A {@link StorageMapper} which selects storage verticles randomly.
 * Replicas are guaranteed to be on different nodes.
 */
public class NodeAwareRandomStorageMapper implements StorageMapper {

    private final Random random;

    public NodeAwareRandomStorageMapper() {
        this.random = new Random();
    }

    public NodeAwareRandomStorageMapper(Random random) {
        this.random = random;
    }

    @Override
    public Set<Integer> select(Collection<DmlNodeInfo> availableNodes, int numReplicas) {
        if (numReplicas < 1) {
            throw new IllegalArgumentException("The number of replicas must be at least 1");
        }
        Map<Integer, DmlNodeInfo> verticleIdToNode = availableNodes.stream()
                .flatMap(node -> node.getVerticles().stream())
                .collect(Collectors.toMap(VerticleInfo::getId, VerticleInfo::getOwnerNode));
        List<Integer> storageCandidates = availableNodes.stream()
                .flatMap(node -> node.getVerticles().stream())
                .filter(verticle -> verticle.getType() == VerticleType.STORAGE)
                .map(VerticleInfo::getId)
                .collect(Collectors.toList());

        Set<Integer> selectedVerticleIds = new LinkedHashSet<>();
        for (int i = 0; i < numReplicas; i++) {
            if (storageCandidates.isEmpty()) {
                throw new IllegalArgumentException("The required number of replicas exceeds the number of available " +
                        "nodes owning storage verticles");
            }
            int selectedVerticle = storageCandidates.get(random.nextInt(storageCandidates.size()));
            selectedVerticleIds.add(selectedVerticle);
            // Remove all verticles owned by the same node as the selected verticle from the candidates
            storageCandidates.removeAll(verticleIdToNode.get(
                    selectedVerticle).getVerticles().stream().map(VerticleInfo::getId).collect(Collectors.toList())
            );
        }
        return selectedVerticleIds;
    }
}
