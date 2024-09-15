package at.uibk.dps.dml.node.metadata;

import io.vertx.core.Future;

public interface MetadataChangeListener {

    Future<Void> keyInvalidated(String key, KeyMetadata oldMetadata, KeyMetadata newMetadata);

    void keyValidated(String key, KeyMetadata metadata);

    void keyOwnerLeft(String key, KeyMetadata metadata);

}
