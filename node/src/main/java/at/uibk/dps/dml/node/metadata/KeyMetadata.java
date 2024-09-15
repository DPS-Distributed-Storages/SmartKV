package at.uibk.dps.dml.node.metadata;

import java.util.Set;

public class KeyMetadata {

    private final Set<Integer> objectLocations;
    private final boolean fullReplication;

    public KeyMetadata(Set<Integer> objectLocations) {
        this.objectLocations = objectLocations;
        this.fullReplication = false;
    }

    public KeyMetadata(Set<Integer> objectLocations, boolean fullReplication) {
        this.objectLocations = objectLocations;
        this.fullReplication = fullReplication;
    }

    public Set<Integer> getObjectLocations() {
        return objectLocations;
    }

    public boolean isFullReplication() {
        return fullReplication;
    }
}
