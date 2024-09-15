package at.uibk.dps.dml.client.storage;

import at.uibk.dps.dml.client.metadata.Storage;

import java.util.List;

public class SimpleStorageSelector implements StorageSelector {

    @Override
    public Storage select(List<Storage> candidates) {
        return candidates.get(0);
    }
}
