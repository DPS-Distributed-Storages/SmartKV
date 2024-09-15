package at.uibk.dps.dml.node;

import at.uibk.dps.dml.node.membership.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.*;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.ext.cluster.infinispan.InfinispanClusterManager;
import io.vertx.micrometer.Label;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxJmxMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * Entry point for the DML node.
 */
public class DmlNodeMain {

    // Environment variable names
    public static final String VERTICLE_BASE_ID_ENV_VAR_NAME = "DML_VERTICLE_BASE_ID";
    public static final String REGION_ENV_VAR_NAME = "DML_REGION";
    public static final String PROVIDER_ENV_VAR_NAME = "DML_PROVIDER";
    public static final String ZONE_ENV_VAR_NAME = "DML_ZONE";
    public static final String HOSTNAME_ENV_VAR_NAME = "DML_HOSTNAME";
    public static final String MEMORY_ENV_VAR_NAME = "DML_MEMORY";
    public static final String REPLICAS_ENV_VAR_NAME = "DML_REPLICAS";
    public static final String ALLOW_REPLICAS_ON_THE_SAME_NODE_ENV_VAR_NAME = "DML_ALLOW_REPLICAS_ON_THE_SAME_NODE";

    public static final String UNIT_STORAGE_PRICE_ENV_VAR_NAME = "DML_UNIT_STORAGE_PRICE";
    public static final String UNIT_REQUEST_PRICE_PUT_ENV_VAR_NAME = "DML_UNIT_REQUEST_PRICE_PUT";
    public static final String UNIT_REQUEST_PRICE_GET_ENV_VAR_NAME = "DML_UNIT_REQUEST_PRICE_GET";
    public static final String UNIT_ACTIVE_KV_PRICE_ENV_VAR_NAME = "DML_UNIT_ACTIVE_KV_PRICE";
    public static final String UNIT_EGRESS_TRANSFER_PRICE_SAME_REGION_ENV_VAR_NAME = "DML_UNIT_EGRESS_TRANSFER_PRICE_SAME_REGION";
    public static final String UNIT_EGRESS_TRANSFER_PRICE_SAME_PROVIDER_ENV_VAR_NAME = "DML_UNIT_EGRESS_TRANSFER_PRICE_SAME_PROVIDER";
    public static final String UNIT_EGRESS_TRANSFER_PRICE_INTERNET_ENV_VAR_NAME = "DML_UNIT_EGRESS_TRANSFER_PRICE_INTERNET";

    public static final String UNIT_INGRESS_TRANSFER_PRICE_SAME_REGION_ENV_VAR_NAME = "DML_UNIT_INGRESS_TRANSFER_PRICE_SAME_REGION";
    public static final String UNIT_INGRESS_TRANSFER_PRICE_SAME_PROVIDER_ENV_VAR_NAME = "DML_UNIT_INGRESS_TRANSFER_PRICE_SAME_PROVIDER";
    public static final String UNIT_INGRESS_TRANSFER_PRICE_INTERNET_ENV_VAR_NAME = "DML_UNIT_INGRESS_TRANSFER_PRICE_INTERNET";

    public static final String IS_METADATA_NODE_ENV_VAR_NAME = "DML_IS_METADATA_NODE";
    public static final String METADATA_PORT_ENV_VAR_NAME = "DML_METADATA_PORT";
    public static final String STORAGE_VERTICLES_ENV_VAR_NAME = "DML_STORAGE_VERTICLES";
    public static final String STORAGE_BASE_PORT_ENV_VAR_NAME = "DML_STORAGE_BASE_PORT";

    public static final String MONITORING_PORT_ENV_VAR_NAME = "DML_PROMETHEUS_MONITORING_PORT";

    public static final int DEFAULT_MONITORING_PORT = 8081;

    public static final String SHARED_VERTICLE_ID_COUNTER_NAME = "verticleIdCounter";
    public static final int DEFAULT_METADATA_PORT = 9000;
    public static final int DEFAULT_STORAGE_BASE_PORT = 9001;
    public static final int DEFAULT_STORAGE_VERTICLES = 4;

    private static final Logger logger = LoggerFactory.getLogger(DmlNodeMain.class);

    /**
     * Starts the DML node.
     *
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        int monitoringPort = Integer.parseInt(System.getenv().getOrDefault(MONITORING_PORT_ENV_VAR_NAME, "" + DEFAULT_MONITORING_PORT));

        InfinispanClusterManager clusterManager = new InfinispanClusterManager();
        VertxOptions options = new VertxOptions()
                .setMaxWorkerExecuteTimeUnit(TimeUnit.MILLISECONDS)
                .setMaxWorkerExecuteTime(600000)
                .setClusterManager(clusterManager)
                .setMetricsOptions(
                        new MicrometerMetricsOptions()
                                .setPrometheusOptions(new VertxPrometheusOptions()
                                        .setEnabled(true)
                                        .setStartEmbeddedServer(true)
                                        .setEmbeddedServerOptions(new HttpServerOptions().setPort(monitoringPort))
                                        .setEmbeddedServerEndpoint("/metrics"))
                                .setEnabled(true)
                                .addLabels(Label.EB_SIDE, Label.EB_ADDRESS, Label.EB_FAILURE, Label.REMOTE, Label.LOCAL, Label.POOL_NAME, Label.POOL_TYPE)
                                .setJvmMetricsEnabled(true));

        // Join the cluster with empty node metadata, we set it when we know the verticle IDs
        options.getEventBusOptions().setClusterNodeMetadata(new JsonObject());

        Vertx.clusteredVertx(options).compose(vertx -> {
            // Load the node configuration
            ConfigRetriever retriever = ConfigRetriever.create(vertx);
            return retriever.getConfig().compose(config -> {
                // Allocate unique verticle IDs
                int numVerticles = isMetadataNode(config) ? getNumStorageVerticles(config) + 1 : getNumStorageVerticles(config);
                return getBaseVerticleId(vertx, config, numVerticles).compose(baseVerticleId -> {
                    // Create node metadata and publish it to all cluster members
                    DmlNodeInfo nodeInfo = getNodeInfo(config, baseVerticleId);

                    // Add Micrometer tags
                    MeterRegistry registry = BackendRegistries.getDefaultNow();
                    if(registry != null) {
                        registry.config().commonTags("sdkv.region", nodeInfo.getRegion());
                        registry.config().commonTags("sdkv.hostname", nodeInfo.getHostname());
                    }

                    return publishNodeMetadata(clusterManager, nodeInfo).compose(v -> {
                        // Start membership manager
                        MembershipManager membershipManager = new MembershipManager(vertx, clusterManager, new SplitBrainHandlerImpl(vertx));
                        return membershipManager.init()
                                // Deploy verticles
                                .compose(v2 -> new VerticleDeployer(vertx, membershipManager, nodeInfo).deployVerticles());
                    });
                });
            }).onFailure(err -> vertx.close());
        }).onFailure(err -> logger.error("Creation of clustered instance failed", err));

    }

    private static DmlNodeInfo getNodeInfo(JsonObject config, int verticleIdCounter) {
        DmlNodeInfo nodeInfo = new DmlNodeInfo();
        // Use json config file for setup if available
        if(config.containsKey("nodeInfo")) {
            JsonObject nodeInfoJson = config.getJsonObject("nodeInfo");
            nodeInfo = nodeInfoJson.mapTo(DmlNodeInfo.class);
            // Still allow some environment variables to override parts of the config file
            nodeInfo.setRegion(config.getString(REGION_ENV_VAR_NAME, nodeInfo.getRegion()));
            nodeInfo.setProvider(config.getString(PROVIDER_ENV_VAR_NAME, nodeInfo.getProvider()));
            nodeInfo.setZone(config.getString(ZONE_ENV_VAR_NAME, nodeInfo.getZone()));
            nodeInfo.setHostname(config.getString(HOSTNAME_ENV_VAR_NAME, nodeInfo.getHostname()));
            nodeInfo.setMemory(config.getInteger(MEMORY_ENV_VAR_NAME, nodeInfo.getMemory()));
            nodeInfo.setDefaultNumReplicas(config.getInteger(REPLICAS_ENV_VAR_NAME, nodeInfo.getDefaultNumReplicas()));
            nodeInfo.setAllowReplicasOnTheSameNode(config.getBoolean(ALLOW_REPLICAS_ON_THE_SAME_NODE_ENV_VAR_NAME,
                    nodeInfo.isAllowReplicasOnTheSameNode()));
        } else {
            // Else check environment variable configs or use default
            nodeInfo.setRegion(config.getString(REGION_ENV_VAR_NAME, nodeInfo.getRegion()));
            nodeInfo.setProvider(config.getString(PROVIDER_ENV_VAR_NAME, nodeInfo.getProvider()));
            nodeInfo.setZone(config.getString(ZONE_ENV_VAR_NAME, nodeInfo.getZone()));
            nodeInfo.setHostname(config.getString(HOSTNAME_ENV_VAR_NAME, nodeInfo.getHostname()));
            nodeInfo.setMemory(config.getInteger(MEMORY_ENV_VAR_NAME, nodeInfo.getMemory()));
            nodeInfo.setDefaultNumReplicas(config.getInteger(REPLICAS_ENV_VAR_NAME, nodeInfo.getDefaultNumReplicas()));
            nodeInfo.setAllowReplicasOnTheSameNode(config.getBoolean(ALLOW_REPLICAS_ON_THE_SAME_NODE_ENV_VAR_NAME,
                    nodeInfo.isAllowReplicasOnTheSameNode()));
            nodeInfo.setUnitPrices(getNodeUnitPrices(config));
            Set<VerticleInfo> verticleInfos = new HashSet<>();
            nodeInfo.setVerticles(verticleInfos);

            if (isMetadataNode(config)) {
                int port = config.getInteger(METADATA_PORT_ENV_VAR_NAME, DEFAULT_METADATA_PORT);
                verticleInfos.add(new VerticleInfo(verticleIdCounter++, VerticleType.METADATA, port, nodeInfo));
            }

            int storagePortSequence = config.getInteger(STORAGE_BASE_PORT_ENV_VAR_NAME, DEFAULT_STORAGE_BASE_PORT);
            for (int i = 0; i < getNumStorageVerticles(config); i++) {
                verticleInfos.add(new VerticleInfo(verticleIdCounter++, VerticleType.STORAGE, storagePortSequence++,
                        nodeInfo));
            }
        }
        return nodeInfo;
    }

    private static DmlNodeUnitPrices getNodeUnitPrices(JsonObject config) {
        DmlNodeUnitPrices dmlNodeUnitPrices = new DmlNodeUnitPrices();
        DmlNodeUnitTransferPrices dmlNodeUnitEgressPrices = new DmlNodeUnitTransferPrices();
        DmlNodeUnitTransferPrices dmlNodeUnitIngressPrices = new DmlNodeUnitTransferPrices();

        dmlNodeUnitEgressPrices.setUnitTransferPriceSameRegion(
                config.getDouble(UNIT_EGRESS_TRANSFER_PRICE_SAME_REGION_ENV_VAR_NAME, dmlNodeUnitEgressPrices.getUnitTransferPriceSameRegion())
        );
        dmlNodeUnitEgressPrices.setUnitTransferPriceSameProvider(
                config.getDouble(UNIT_EGRESS_TRANSFER_PRICE_SAME_PROVIDER_ENV_VAR_NAME, dmlNodeUnitEgressPrices.getUnitTransferPriceSameProvider())
        );
        dmlNodeUnitEgressPrices.setUnitTransferPriceInternet(
                config.getDouble(UNIT_EGRESS_TRANSFER_PRICE_INTERNET_ENV_VAR_NAME, dmlNodeUnitEgressPrices.getUnitTransferPriceInternet())
        );

        dmlNodeUnitIngressPrices.setUnitTransferPriceSameRegion(
                config.getDouble(UNIT_INGRESS_TRANSFER_PRICE_SAME_REGION_ENV_VAR_NAME, dmlNodeUnitIngressPrices.getUnitTransferPriceSameRegion())
        );
        dmlNodeUnitIngressPrices.setUnitTransferPriceSameProvider(
                config.getDouble(UNIT_INGRESS_TRANSFER_PRICE_SAME_PROVIDER_ENV_VAR_NAME, dmlNodeUnitIngressPrices.getUnitTransferPriceSameProvider())
        );
        dmlNodeUnitIngressPrices.setUnitTransferPriceInternet(
                config.getDouble(UNIT_INGRESS_TRANSFER_PRICE_INTERNET_ENV_VAR_NAME, dmlNodeUnitIngressPrices.getUnitTransferPriceInternet())
        );

        dmlNodeUnitPrices.setUnitStoragePrice(config.getDouble(UNIT_STORAGE_PRICE_ENV_VAR_NAME, dmlNodeUnitPrices.getUnitStoragePrice()));
        dmlNodeUnitPrices.setUnitRequestPricePUT(config.getDouble(UNIT_REQUEST_PRICE_PUT_ENV_VAR_NAME, dmlNodeUnitPrices.getUnitRequestPricePUT()));
        dmlNodeUnitPrices.setUnitRequestPriceGET(config.getDouble(UNIT_REQUEST_PRICE_GET_ENV_VAR_NAME, dmlNodeUnitPrices.getUnitRequestPriceGET()));
        dmlNodeUnitPrices.setUnitActiveKVPrice(config.getDouble(UNIT_ACTIVE_KV_PRICE_ENV_VAR_NAME, dmlNodeUnitPrices.getUnitActiveKVPrice()));
        dmlNodeUnitPrices.setUnitEgressTransferPrices(dmlNodeUnitEgressPrices);
        dmlNodeUnitPrices.setUnitIngressTransferPrices(dmlNodeUnitIngressPrices);

        return dmlNodeUnitPrices;
    }

    private static boolean isMetadataNode(JsonObject config) {
        return config.getBoolean(IS_METADATA_NODE_ENV_VAR_NAME, true) || config.containsKey(METADATA_PORT_ENV_VAR_NAME);
    }

    private static int getNumStorageVerticles(JsonObject config) {
        return config.getInteger(STORAGE_VERTICLES_ENV_VAR_NAME, DEFAULT_STORAGE_VERTICLES);
    }

    private static Future<Integer> getBaseVerticleId(Vertx vertx, JsonObject config, int numVerticles) {
        if (!config.containsKey(VERTICLE_BASE_ID_ENV_VAR_NAME)) {
            // Get unique verticle IDs from a shared counter
            return vertx.sharedData()
                    .getCounter(SHARED_VERTICLE_ID_COUNTER_NAME)
                    .compose(counter -> counter.getAndAdd(numVerticles))
                    .map(counter -> counter == null ? null : Math.toIntExact(counter));
        } else {
            return Future.succeededFuture(config.getInteger(VERTICLE_BASE_ID_ENV_VAR_NAME));
        }
    }

    private static Future<Void> publishNodeMetadata(ClusterManager clusterManager, DmlNodeInfo nodeInfo) {
        NodeInfo vertxNodeInfo = clusterManager.getNodeInfo();
        vertxNodeInfo.metadata().mergeIn(JsonObject.mapFrom(nodeInfo));
        Promise<Void> setNodeInfoPromise = Promise.promise();
        clusterManager.setNodeInfo(vertxNodeInfo, setNodeInfoPromise);
        return setNodeInfoPromise.future();
    }
}
