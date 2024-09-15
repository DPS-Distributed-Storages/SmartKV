package at.uibk.dps.dml.node.metadata;

import at.uibk.dps.dml.node.membership.DmlNodeInfo;

import java.util.Collection;
import java.util.Set;

public interface StorageMapper {

    /**
     * Chooses n (numReplicas) storage verticles from the available nodes.
     *
     * @param availableNodes the DML nodes of the current membership view
     * @param numReplicas the number of required replicas
     * @return the selected storage verticle IDs
     */
    Set<Integer> select(Collection<DmlNodeInfo> availableNodes, int numReplicas);

}
