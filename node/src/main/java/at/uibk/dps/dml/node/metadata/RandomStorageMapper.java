package at.uibk.dps.dml.node.metadata;

import at.uibk.dps.dml.node.membership.DmlNodeInfo;
import at.uibk.dps.dml.node.membership.VerticleInfo;
import at.uibk.dps.dml.node.membership.VerticleType;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A {@link StorageMapper} which selects storage verticles randomly.
 */
public class RandomStorageMapper implements StorageMapper {

    private final Random random;

    public RandomStorageMapper() {
        this.random = new Random();
    }

    public RandomStorageMapper(Random random) {
        this.random = random;
    }

    @Override
    public Set<Integer> select(Collection<DmlNodeInfo> availableNodes, int numReplicas) {
        if (numReplicas < 1) {
            throw new IllegalArgumentException("The number of replicas must be at least 1");
        }
        List<Integer> storageCandidates = availableNodes.stream()
                .flatMap(node -> node.getVerticles().stream())
                .filter(verticle -> verticle.getType() == VerticleType.STORAGE)
                .map(VerticleInfo::getId)
                .collect(Collectors.toList());
        if (numReplicas > storageCandidates.size()) {
            throw new IllegalArgumentException("The required number of replicas exceeds the number of available storage verticles");
        }

        Collections.shuffle(storageCandidates, random);

        return new LinkedHashSet<>(storageCandidates.subList(0, numReplicas));
    }
}
