package at.uibk.dps.dml.node.storage;

import at.uibk.dps.dml.node.RpcRouter;
import at.uibk.dps.dml.node.billing.BillingService;
import at.uibk.dps.dml.node.membership.MembershipManager;
import at.uibk.dps.dml.node.membership.VerticleInfo;
import at.uibk.dps.dml.node.metadata.MetadataService;
import at.uibk.dps.dml.node.metadata.rpc.MetadataRpcHandler;
import at.uibk.dps.dml.node.metadata.rpc.MetadataRpcService;
import at.uibk.dps.dml.node.metadata.rpc.MetadataRpcServiceImpl;
import at.uibk.dps.dml.node.statistics.OptimizerRequestHandler;
import at.uibk.dps.dml.node.statistics.StatisticManager;
import at.uibk.dps.dml.node.storage.rpc.StorageRpcHandler;
import at.uibk.dps.dml.node.storage.rpc.StorageRpcService;
import at.uibk.dps.dml.node.storage.rpc.StorageRpcServiceImpl;
import at.uibk.dps.dml.node.util.NodeLocation;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.net.SocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class StorageServerVerticle extends AbstractVerticle {

    private final Logger logger = LoggerFactory.getLogger(StorageServerVerticle.class);

    private final MembershipManager membershipManager;

    private final VerticleInfo verticleInfo;

    private final SharedObjectFactory sharedObjectFactory;

    /**
     * Caches the {@link NodeLocation} for each connected client
     */
    private final Map<SocketAddress, NodeLocation> clientLocationMap;


    private static final int STATISTIC_SERVER_BASE_PORT = 7000;
    private final int STATISTIC_SERVER_PORT;

    public StorageServerVerticle(MembershipManager membershipManager, VerticleInfo verticleInfo,
                                 SharedObjectFactory sharedObjectFactory) {
        this.membershipManager = membershipManager;
        this.verticleInfo = verticleInfo;
        this.sharedObjectFactory = sharedObjectFactory;
        this.clientLocationMap = new HashMap<>();

        this.STATISTIC_SERVER_PORT = STATISTIC_SERVER_BASE_PORT + verticleInfo.getId();

    }

    @Override
    public void start(Promise<Void> startPromise) {
        BillingService storageBillingService = new BillingService(vertx, membershipManager, verticleInfo.getOwnerNode().getRegion(), verticleInfo.getOwnerNode().getProvider(), verticleInfo.getId(), verticleInfo.getOwnerNode().getUnitPrices());
        MetadataRpcService metadataRpcService = new MetadataRpcServiceImpl(vertx);
        MetadataService metadataService = new MetadataService(vertx, membershipManager,
                verticleInfo, metadataRpcService, null);
        MetadataRpcHandler metadataRpcHandler = new MetadataRpcHandler(metadataService);
        StorageRpcService storageRpcService = new StorageRpcServiceImpl(vertx, verticleInfo.getId(), storageBillingService);
        StorageService storageService = new StorageService(
                vertx, verticleInfo.getId(),
                verticleInfo.getOwnerNode().getMemory(),
                sharedObjectFactory,
                storageRpcService, metadataService, storageBillingService, clientLocationMap);
        StatisticManager statisticManager = new StatisticManager(storageService);
        StorageRpcHandler storageRpcHandler = new StorageRpcHandler(storageService, storageBillingService);
        vertx.eventBus().consumer(String.valueOf(verticleInfo.getId()), new RpcRouter(metadataRpcHandler, storageRpcHandler))
                .completionHandler(eventbusRes -> {
                    if (eventbusRes.failed()) {
                        logger.error("Eventbus registration of verticle {} failed", verticleInfo.getId(), eventbusRes.cause());
                        startPromise.fail(eventbusRes.cause());
                        return;
                    }
                    logger.info("Eventbus registration of verticle {} has reached all nodes", verticleInfo.getId());

                    vertx.createNetServer()
                            .connectHandler(new TcpRequestHandler(storageService, metadataService, storageBillingService, statisticManager))
                            .listen(verticleInfo.getPort())
                            .onSuccess(netServer -> logger.info("Storage server {} is now listening to port {}", verticleInfo.getId(), verticleInfo.getPort()))
                            .onFailure(err -> {
                                logger.error("Storage server {} failed to bind to port {}", verticleInfo.getId(), verticleInfo.getPort(), err);
                                startPromise.fail(eventbusRes.cause());
                            });

                    vertx.createNetServer()
                            .connectHandler(new OptimizerRequestHandler(statisticManager, verticleInfo.getOwnerNode().getZone()))
                            .listen(STATISTIC_SERVER_PORT)
                            .onSuccess(netServer -> logger.info("Storage statistics server {} is now listening to port {}", verticleInfo.getId(), STATISTIC_SERVER_PORT))
                            .onFailure(err -> {
                                logger.error("Storage statistics server {} failed to bind to port {}", verticleInfo.getId(), STATISTIC_SERVER_PORT, err);
                                startPromise.fail(eventbusRes.cause());
                            });
                });

    }

    public Map<SocketAddress, NodeLocation> getClientLocationMap() {
        return clientLocationMap;
    }

}
