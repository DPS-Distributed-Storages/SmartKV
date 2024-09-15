package at.uibk.dps.dml.node;

import at.uibk.dps.dml.client.storage.BsonArgsCodec;
import at.uibk.dps.dml.node.membership.DmlNodeInfo;
import at.uibk.dps.dml.node.membership.MembershipManager;
import at.uibk.dps.dml.node.membership.VerticleInfo;
import at.uibk.dps.dml.node.metadata.MetadataServerVerticle;
import at.uibk.dps.dml.node.storage.SharedObjectFactoryImpl;
import at.uibk.dps.dml.node.storage.StorageServerVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.List;

/**
 * The service responsible for deploying the verticles.
 */
public class VerticleDeployer {

    private final Vertx vertx;
    private final MembershipManager membershipManager;
    private final DmlNodeInfo nodeInfo;

    /**
     * Creates a new verticle deployer.
     *
     * @param vertx the vertx instance
     * @param membershipManager the membership manager
     * @param nodeInfo the node info
     */
    public VerticleDeployer(Vertx vertx, MembershipManager membershipManager, DmlNodeInfo nodeInfo) {
        this.vertx = vertx;
        this.membershipManager = membershipManager;
        this.nodeInfo = nodeInfo;
    }

    /**
     * Deploys the verticles specified in the node info.
     *
     * @return a future which completes when all verticles are deployed
     */
    public Future<Void> deployVerticles() {
        @SuppressWarnings("rawtypes")
        List<Future> deployFutures = new ArrayList<>();
        for (VerticleInfo verticleInfo : nodeInfo.getVerticles()) {
            switch (verticleInfo.getType()) {
                case METADATA:
                    deployFutures.add(
                            vertx.deployVerticle(new MetadataServerVerticle(membershipManager, verticleInfo)));
                    break;
                case STORAGE:
                    deployFutures.add(
                            vertx.deployVerticle(new StorageServerVerticle(
                            membershipManager, verticleInfo, new SharedObjectFactoryImpl(new BsonArgsCodec(), vertx))));
                    break;
                default:
                    return Future.failedFuture("Invalid verticle type");
            }
        }
        return CompositeFuture.join(deployFutures).mapEmpty();
    }
}
