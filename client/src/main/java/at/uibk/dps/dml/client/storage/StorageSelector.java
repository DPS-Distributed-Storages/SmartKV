package at.uibk.dps.dml.client.storage;

import at.uibk.dps.dml.client.metadata.Storage;

import java.util.List;

public interface StorageSelector {

    Storage select(List<Storage> candidates);

}
