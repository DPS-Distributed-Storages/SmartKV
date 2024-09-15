package at.uibk.dps.dml.node.metadata;

import at.uibk.dps.dml.client.metadata.KeyConfiguration;
import at.uibk.dps.dml.client.metadata.Storage;
import at.uibk.dps.dml.node.exception.*;
import at.uibk.dps.dml.node.membership.*;
import at.uibk.dps.dml.node.storage.rpc.StorageRpcService;
import at.uibk.dps.dml.node.util.Timestamp;
import at.uibk.dps.dml.node.metadata.command.*;
import at.uibk.dps.dml.node.metadata.rpc.*;
import at.uibk.dps.dml.node.util.ValidatorChain;
import at.uibk.dps.dml.node.util.ZoneInfo;
import io.vertx.core.*;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class MetadataService implements CommandHandler {

    private final Logger logger = LoggerFactory.getLogger(MetadataService.class);

    private final Vertx vertx;

    private final VerticleInfo verticleInfo;

    private final MetadataRpcService metadataRpcService;

    private final StorageMapper storageMapper;
    /**
     * Maps keys to pending commands
     */
    private final Map<String, Queue<PendingCommand>> pendingCommands = new HashMap<>();

    private final Map<String, MetadataEntry> keyMetadataMap = new HashMap<>();

    private MembershipView membershipView;

    /**
     * Contains the verticle IDs of all metadata verticles in the current membership view.
     */
    private Set<Integer> metadataVerticleIds;

    /**
     * Contains the verticle IDs of all storage verticles per zone.
     */
    private Map<String, Set<Integer>> storageVerticleIdsPerZone;

    /**
     * Contains the verticle IDs of all metadata verticles per zone.
     */
    private Map<String, Set<Integer>> metadataVerticleIdsPerZone;

    private MetadataChangeListener listener;

    private int requestCounter = 0;

    private final StorageRpcService storageRpcService;

    private final Random rand;
    
    private final int MAX_RETRIES = 3;

    public MetadataService(Vertx vertx, MembershipManager membershipManager,
                           VerticleInfo verticleInfo, MetadataRpcService metadataRpcService,
                           StorageMapper storageMapper, StorageRpcService storageRpcService) {
        this.vertx = vertx;
        this.verticleInfo = verticleInfo;
        this.metadataRpcService = metadataRpcService;
        this.storageRpcService = storageRpcService;
        this.storageMapper = storageMapper;
        this.rand = new Random();
        membershipManager.addListener(view -> vertx.runOnContext(event -> onMembershipChange(view)));
        onMembershipChange(membershipManager.getMembershipView());
    }

    public MetadataService(Vertx vertx, MembershipManager membershipManager,
                           VerticleInfo verticleInfo, MetadataRpcService metadataRpcService,
                           StorageMapper storageMapper){
        this(vertx, membershipManager, verticleInfo, metadataRpcService, storageMapper, null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Future<Void> create(String key, Set<Integer> objectLocations, boolean fullReplication) {
        if (keyMetadataMap.containsKey(key)) {
            return Future.failedFuture(new KeyAlreadyExistsException());
        }

        if ((objectLocations == null || objectLocations.isEmpty()) && !fullReplication) {
            objectLocations = storageMapper.select(
                    membershipView.getNodeMap().values(),
                    verticleInfo.getOwnerNode().getDefaultNumReplicas());
            }else if (fullReplication){
                objectLocations = membershipView.getVerticleIdsByType(VerticleType.STORAGE);
            } else if (!membershipView.getVerticleIdsByType(VerticleType.STORAGE).containsAll(objectLocations)) {
            return Future.failedFuture(new InvalidObjectLocationsException());
        }

        createMetadataEntry(key, new Timestamp(0, verticleInfo.getId()));

        Promise promise = Promise.promise();
        enqueueCommand(new CreateCommand(promise, key, new KeyMetadata(objectLocations, fullReplication)));
        return promise.future();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Future<KeyConfiguration> get(String key) {
        MetadataEntry metadataEntry = keyMetadataMap.get(key);
        if (metadataEntry == null) {
            return Future.failedFuture(new KeyDoesNotExistException());
        }
        Promise promise = Promise.promise();
        enqueueCommand(new GetCommand(promise, key));
        return promise.future();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Future<Map<String, KeyConfiguration>> getAll() {
        Promise promise = Promise.promise();
        enqueueCommand(new GetAllCommand(promise));
        return promise.future();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Future<Void> reconfigure(String key, Set<Integer> objectLocations) {
        if (!membershipView.getVerticleIdsByType(VerticleType.STORAGE).containsAll(objectLocations)) {
            return Future.failedFuture(new InvalidObjectLocationsException());
        }
        Promise promise = Promise.promise();
        enqueueCommand(new ReconfigureCommand(promise, key, new KeyMetadata(objectLocations)));
        return promise.future();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Future<Void> synchronizedReconfigure(String key, Set<Integer> oldObjectLocations, Set<Integer> newObjectLocations) {
        if (!membershipView.getVerticleIdsByType(VerticleType.STORAGE).containsAll(newObjectLocations)) {
            return Future.failedFuture(new InvalidObjectLocationsException());
        }
        Promise promise = Promise.promise();
        enqueueCommand(new SynchronizedReconfigureCommand(promise, key, new KeyMetadata(oldObjectLocations), new KeyMetadata(newObjectLocations)));
        return promise.future();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Future<Void> delete(String key) {
        Promise promise = Promise.promise();
        enqueueCommand(new DeleteCommand(promise, key));
        return promise.future();
    }

    public MembershipView getMembershipView() {
        return membershipView;
    }

    public Future<ZoneInfo> getZoneInfo(String zone){
        if(!metadataVerticleIdsPerZone.containsKey(zone) && !storageVerticleIdsPerZone.containsKey(zone)){
            return Future.failedFuture(new MetadataRpcException(MetadataRpcErrorType.NO_METADATA_IN_ZONE, "Zone " + zone + " does not exist!"));
        }
        // Handle the command locally, if the local zone is the zone of interest.
        else if(zone.equals(this.verticleInfo.getOwnerNode().getZone())){
            return getZoneInfoLocal();
        }
        // Redirect this command to a MetadataServer in the zone of interest, if a MetadataServer exists there!
        else if(metadataVerticleIdsPerZone.containsKey(zone) && !metadataVerticleIdsPerZone.get(zone).isEmpty()){
            Promise<ZoneInfo> promise = Promise.promise();
            retrieveZoneInfoFromRemote(promise, zone, 0);
            return promise.future();
        }
        // If there is no MetadataServer in the zone of interest, simply handle the command here.
        else return getZoneInfoLocal();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Future<Set<Integer>> selectFreeStoragesFromZones(Set<String> zones, long objectSizeInBytes){
        Promise finalPromise = Promise.promise();
        List<Future> rpcFutures = new ArrayList<>();
        for(String zone : zones) {
            if (!storageVerticleIdsPerZone.containsKey(zone)) {
                return Future.failedFuture(new MetadataRpcException(MetadataRpcErrorType.NO_METADATA_IN_ZONE, "Zone " + zone + " does not exist!"));
            }
            // Handle the command locally, if the local zone is the zone of interest.
            else if (zone.equals(this.verticleInfo.getOwnerNode().getZone())) {
                rpcFutures.add(selectFreeStorageFromZoneLocal(zone, objectSizeInBytes));
            }
            // Redirect this command to a MetadataServer in the zone of interest, if a MetadataServer exists there!
            else if (metadataVerticleIdsPerZone.containsKey(zone) && !metadataVerticleIdsPerZone.get(zone).isEmpty()) {
                Promise promise = Promise.promise();
                selectFreeStorageFromZoneRemote(promise, zone, objectSizeInBytes, 0);
                rpcFutures.add(promise.future());
            }
            // If there is no MetadataServer in the zone of interest, simply handle the command here.
            else rpcFutures.add(selectFreeStorageFromZoneLocal(zone, objectSizeInBytes));
        }

        CompositeFuture.all(rpcFutures)
                .onSuccess(res ->
                {
                    finalPromise.complete(new LinkedHashSet<>(res.result().list()));
                })
                .onFailure(err -> {
                    logger.debug("Failed to retrieve free storage nodes from zones!", err);
                    finalPromise.fail(err);
                });

        return finalPromise.future();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Future<Integer> selectFreeStorageFromZoneLocal(String zone, long objectSizeInBytes){
        Promise promise = Promise.promise();
        List<Integer> zoneVerticleIds = new ArrayList(storageVerticleIdsPerZone.get(zone));

        // Retrieve the memory from all storage nodes in the zone of interest.
        this.getStorageMemory(zoneVerticleIds)
                .onSuccess(freeMemoryPerVerticle -> {
                    promise.complete(selectStorageNode(freeMemoryPerVerticle, objectSizeInBytes));
                })
                .onFailure(err -> promise.fail((Throwable) err));

        return promise.future();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void selectFreeStorageFromZoneRemote(Promise promise, String zone, long objectSizeInBytes, int attempt){
       
        int metadataVerticleId = metadataVerticleIdsPerZone.get(zone).iterator().next();
        metadataRpcService.getFreeStorageNode(metadataVerticleId, zone, objectSizeInBytes)
                .onSuccess(promise::complete)
                .onFailure(err -> {
                    logger.debug("Failed to retrieve zone info from remote metadata server!", err);
                    // Wait a bit and retry
                    if(attempt < MAX_RETRIES) {
                        vertx.setTimer(1000, timerId -> selectFreeStorageFromZoneRemote(promise, zone, objectSizeInBytes, attempt + 1));
                    }else{
                        promise.fail(err);
                    }
                });
    }

    private Integer selectStorageNode(Map<Integer, Long> freeMemoryPerVerticle, long objectSizeInBytes){
        List<Map.Entry<Integer, Long>> filteredEntries = freeMemoryPerVerticle.entrySet().stream()
                .filter(entry -> entry.getValue() > objectSizeInBytes * 10)
                .collect(Collectors.toList());

        // If no storage node is available, return -1
        if (filteredEntries.isEmpty()) {
            return -1; 
        }

        // Shuffle the filtered entries
        Collections.shuffle(filteredEntries, rand);

        return filteredEntries.get(0).getKey();
    }

    // If this Metadata node is not responsible for the zone, redirect it to a responsible Metadata node!
    private void retrieveZoneInfoFromRemote(Promise<ZoneInfo> promise, String zone, int attempt) {
        int metadataVerticleId = metadataVerticleIdsPerZone.get(zone).iterator().next();
        metadataRpcService.getZoneInfo(metadataVerticleId)
                .onSuccess(promise::complete)
                .onFailure(err -> {
                    logger.debug("Failed to retrieve zone info from remote metadata server!", err);
                    if(attempt < MAX_RETRIES) {                    // Wait a bit and retry
                        vertx.setTimer(1000, timerId -> retrieveZoneInfoFromRemote(promise, zone, attempt + 1));
                    }else{
                        promise.fail(err);
                    }
                });
    }

    /**
     * Returns information about the zone of interest.
     * This info includes the currently total free memory per zone and the average prices per zone.
     * The average zone price is calculated as the average over all storage node prices, weighted by their respective free memory.
     * So it returns the average prices per byte in the zone.
     * @return the zone info.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Future<ZoneInfo> getZoneInfoLocal() {
        Promise promise = Promise.promise();
        List<Integer> zoneVerticleIds = new ArrayList(storageVerticleIdsPerZone.get(this.verticleInfo.getOwnerNode().getZone()));

        // Retrieve the zone info from all storage nodes in the same zone as this Metadata server!
        this.getStorageMemory(zoneVerticleIds)
                .onSuccess(freeMemoryPerVerticle -> {
                    long totalFreeMemoryOfZone = freeMemoryPerVerticle.values().stream().mapToLong(Long::longValue).sum();
                    DmlNodeUnitPrices weightedAverageZonePrices = calculateWeightedAverageZonePrice(freeMemoryPerVerticle, totalFreeMemoryOfZone);
                    promise.complete(new ZoneInfo(weightedAverageZonePrices, totalFreeMemoryOfZone));
                })
                .onFailure(err -> promise.fail((Throwable) err));

        return promise.future();
    }

    private DmlNodeUnitPrices calculateWeightedAverageZonePrice(Map<Integer, Long> freeMemoryPerVerticle, long totalFreeMemoryOfZone){
        DmlNodeUnitPrices weightedAvgZonePrice = new DmlNodeUnitPrices();
        for(Map.Entry<Integer, Long> entry : freeMemoryPerVerticle.entrySet()){
            VerticleInfo verticle = membershipView.findVerticleById(entry.getKey());
            DmlNodeUnitPrices weightedUnitPricesVerticle = verticle.getOwnerNode().getUnitPrices();
            // Weight the current unit price of this verticle by (freeMemory / totalMemoryInZone)
            weightedUnitPricesVerticle = calculateWeightedStoragePrice(weightedUnitPricesVerticle, entry.getValue(), totalFreeMemoryOfZone);
            // add the weighted price to the total zone price to get the average prices per zone
            addUnitPrice(weightedAvgZonePrice, weightedUnitPricesVerticle);
        }
        return weightedAvgZonePrice;
    }

    private void addUnitPrice(DmlNodeUnitPrices unitPrices, DmlNodeUnitPrices unitPricesToAdd){
        unitPrices.setUnitStoragePrice(unitPrices.getUnitStoragePrice() + unitPricesToAdd.getUnitStoragePrice());
        unitPrices.setUnitRequestPricePUT(unitPrices.getUnitRequestPricePUT() + unitPricesToAdd.getUnitRequestPricePUT());
        unitPrices.setUnitRequestPriceGET(unitPrices.getUnitRequestPriceGET() + unitPricesToAdd.getUnitRequestPriceGET());
        unitPrices.setUnitActiveKVPrice(unitPrices.getUnitActiveKVPrice() + unitPricesToAdd.getUnitActiveKVPrice());
        DmlNodeUnitTransferPrices egressPrices = unitPrices.getUnitEgressTransferPrices();
        DmlNodeUnitTransferPrices egressPricesToAdd = unitPricesToAdd.getUnitEgressTransferPrices();
        DmlNodeUnitTransferPrices ingressPrices = unitPrices.getUnitIngressTransferPrices();
        DmlNodeUnitTransferPrices ingressPricesToAdd = unitPricesToAdd.getUnitIngressTransferPrices();
        Arrays.stream(DataTransferPricingCategory.values()).forEach(type -> {
            egressPrices.setUnitTransferPrice(type, egressPrices.getUnitTransferPrice().get(type) + egressPricesToAdd.getUnitTransferPrice().get(type));
            ingressPrices.setUnitTransferPrice(type, ingressPrices.getUnitTransferPrice().get(type) + ingressPricesToAdd.getUnitTransferPrice().get(type));
        });
    }

    private DmlNodeUnitPrices calculateWeightedStoragePrice(DmlNodeUnitPrices unitPrices, long freeMemoryOfStorage, long totalFreeMemoryOfZone){
        DmlNodeUnitPrices multipliedPrices = new DmlNodeUnitPrices();
        multipliedPrices.setUnitStoragePrice(unitPrices.getUnitStoragePrice() * ((double) freeMemoryOfStorage / totalFreeMemoryOfZone));
        multipliedPrices.setUnitRequestPricePUT(unitPrices.getUnitRequestPricePUT() * ((double) freeMemoryOfStorage / totalFreeMemoryOfZone));
        multipliedPrices.setUnitRequestPriceGET(unitPrices.getUnitRequestPriceGET() * ((double) freeMemoryOfStorage / totalFreeMemoryOfZone));
        multipliedPrices.setUnitActiveKVPrice(unitPrices.getUnitActiveKVPrice() * ((double) freeMemoryOfStorage / totalFreeMemoryOfZone));

        // Multiply unit transfer prices
        DmlNodeUnitTransferPrices multipliedEgressTransferPrices = new DmlNodeUnitTransferPrices();
        Arrays.stream(DataTransferPricingCategory.values()).forEach(type -> {
            multipliedEgressTransferPrices.getUnitTransferPrice().put(type, unitPrices.getUnitEgressTransferPrices().getUnitTransferPrice().get(type) * ((double) freeMemoryOfStorage / totalFreeMemoryOfZone));
        });
        multipliedPrices.setUnitEgressTransferPrices(multipliedEgressTransferPrices);

        // Multiply unit transfer prices
        DmlNodeUnitTransferPrices multipliedIngressTransferPrices = new DmlNodeUnitTransferPrices();
        Arrays.stream(DataTransferPricingCategory.values()).forEach(type -> {
            multipliedIngressTransferPrices.getUnitTransferPrice().put(type, unitPrices.getUnitIngressTransferPrices().getUnitTransferPrice().get(type) * ((double) freeMemoryOfStorage / totalFreeMemoryOfZone));
        });
        multipliedPrices.setUnitIngressTransferPrices(multipliedIngressTransferPrices);

        return multipliedPrices;
    }

    // Get information about the storage nodes, including their free memory and their unit prices.
    private Future<Map<Integer, Long>> getStorageMemory(List<Integer> verticleIds) {
        Promise<Map<Integer, Long>> promise = Promise.promise();
        retrieveStorageMemoryWithRetries(verticleIds, promise, 0);
        return promise.future();
    }

    private void retrieveStorageMemoryWithRetries(List<Integer> verticleIds, Promise<Map<Integer, Long>> promise, int attempt) {
       
        @SuppressWarnings("rawtypes")
        List<Future> rpcFutures = new ArrayList<>();
        for (Integer location : verticleIds) {
            rpcFutures.add(storageRpcService.getFreeMemory(location, membershipView.getEpoch()));
        }
        CompositeFuture.all(rpcFutures)
                .onSuccess(res -> {
                    List<Long> resultList = res.list();
                    Map<Integer, Long> resultMap = new HashMap<>();
                    for (int i = 0; i < resultList.size(); i++) {
                        resultMap.put(verticleIds.get(i), resultList.get(i));
                    }
                    promise.complete(resultMap);
                })
                .onFailure(err -> {
                    logger.debug("Failed to retrieve free memory from verticles!", err);
                    if(attempt < MAX_RETRIES) {
                        // Wait a bit and retry
                        vertx.setTimer(1000, timerId -> retrieveStorageMemoryWithRetries(verticleIds, promise, attempt + 1));
                    }else{
                        promise.fail(err);
                    }
                });
    }

    public MetadataEntry getMetadataEntry(String key) {
        return keyMetadataMap.get(key);
    }

    public Future<Void> invalidate(int originEpoch, Timestamp timestamp, InvalidationCommand command) {
        Throwable epochError = checkOriginEpoch(originEpoch);
        if (epochError != null) {
            return Future.failedFuture(epochError);
        }

        MetadataEntry metadataEntry = keyMetadataMap.get(command.getKey());
        if (metadataEntry == null
                && (command.getType() == InvalidationCommandType.CREATE
                || command.getType() == InvalidationCommandType.RECONFIGURE
                || command.getType() == InvalidationCommandType.SYNCHRONIZED_RECONFIGURE)) {
            metadataEntry = createMetadataEntry(command.getKey(), new Timestamp(0, verticleInfo.getId()));
        } else if (metadataEntry == null) {
            return Future.failedFuture(new KeyDoesNotExistException());
        }

        Timestamp localTimestamp = metadataEntry.getTimestamp();
        if (!timestamp.isGreaterThan(localTimestamp)) {
            // We already received a command with a higher timestamp, we simply ACK it
            return Future.succeededFuture();
        }

        metadataEntry.setState(MetadataEntryState.INVALID);
        localTimestamp.setVersion(timestamp.getVersion());
        localTimestamp.setCoordinatorVerticleId(timestamp.getCoordinatorVerticleId());
        metadataEntry.setOldMetadata(command.getOldMetadata());

        try {
            command.apply(this);
        } catch (Exception e) {
            return Future.failedFuture(e);
        }

        if (timestamp.equals(localTimestamp) && listener != null) {
            return listener.keyInvalidated(command.getKey(), metadataEntry.getOldMetadata(), metadataEntry.getMetadata());
        }

        return Future.succeededFuture();
    }

    public Future<Void> commit(int originEpoch, String key, Timestamp timestamp) {
        MetadataEntry metadataEntry = keyMetadataMap.get(key);
        Optional<Throwable> error = new ValidatorChain<Throwable>()
                .validate(() -> checkOriginEpoch(originEpoch))
                .validate(() -> checkKeyExists(metadataEntry))
                .getError();
        if (error.isPresent()) {
            return Future.failedFuture(error.get());
        }

        commitIfTimestampIsMostRecent(key, metadataEntry, timestamp);

        return Future.succeededFuture();
    }

    public void setListener(MetadataChangeListener listener) {
        this.listener = listener;
    }

    @Override
    public Object apply(CreateCommand command) {
        MetadataEntry metadataEntry = keyMetadataMap.get(command.getKey());
        metadataEntry.setMetadata(command.getMetadata());
        return null;
    }

    @Override
    public Object apply(GetCommand command) {
        MetadataEntry metadataEntry = keyMetadataMap.get(command.getKey());
        return getConfigurationForKey(metadataEntry);
    }

    @Override
    public Object apply(GetAllCommand command) {
        Map<String, KeyConfiguration> result = new HashMap<>();
        for (Map.Entry<String, MetadataEntry> entry : keyMetadataMap.entrySet()) {
            result.put(entry.getKey(), getConfigurationForKey(entry.getValue()));
        }
        return result;
    }

    @Override
    public Object apply(ReconfigureCommand command) {
        MetadataEntry metadataEntry = keyMetadataMap.get(command.getKey());
        metadataEntry.setMetadata(command.getNewMetadata());
        return null;
    }

    @Override
    public Object apply(SynchronizedReconfigureCommand command) {
        MetadataEntry currentMetadataEntry = keyMetadataMap.get(command.getKey());
        KeyMetadata seenMetadataEntry = command.getSeenMetadata();
        KeyMetadata newMetadataEntry = command.getNewMetadata();
        // Calculate the correct reconfiguration based on the last observed configuration
        // and the intended configuration (i.e. which replicas were removed or added from the last observed once!)
        if(seenMetadataEntry != null) {
            // We need a deep copy here!
            KeyMetadata updatedMetadata = new KeyMetadata(new HashSet<>(currentMetadataEntry.getMetadata().getObjectLocations()));
            // Remove replicas
            Set<Integer> replicasToRemove = new HashSet<>(seenMetadataEntry.getObjectLocations());
            replicasToRemove.removeAll(newMetadataEntry.getObjectLocations());
            updatedMetadata.getObjectLocations().removeAll(replicasToRemove);
            // Add replicas
            Set<Integer> replicasToAdd = new HashSet<>(newMetadataEntry.getObjectLocations());
            replicasToAdd.removeAll(seenMetadataEntry.getObjectLocations());
            updatedMetadata.getObjectLocations().addAll(replicasToAdd);
            currentMetadataEntry.setMetadata(updatedMetadata);
        }else {
            currentMetadataEntry.setMetadata(newMetadataEntry);
        }
        return null;
    }

    @Override
    public Object apply(DeleteCommand command) {
        MetadataEntry metadataEntry = keyMetadataMap.get(command.getKey());
        metadataEntry.setMetadata(new KeyMetadata(Collections.emptySet()));
        return null;
    }

    private MetadataEntry createMetadataEntry(String key, Timestamp timestamp) {
        MetadataEntry metadataEntry = new MetadataEntry(timestamp);
        metadataEntry.setState(MetadataEntryState.INVALID);
        keyMetadataMap.put(key, metadataEntry);
        return metadataEntry;
    }

    private void enqueueCommand(Command command) {
        if (command.getKey() == null && command.isReadOnly() && command.isAllowInvalidReadsEnabled()) {
            // No need to queue this command, we can execute it immediately
            coordinateCommand(command);
            return;
        } else if (command.getKey() == null) {
            command.getPromise().fail(new IllegalArgumentException("Key must not be null"));
            return;
        }

        MetadataEntry metadataEntry = keyMetadataMap.get(command.getKey());
        Optional<Throwable> error = new ValidatorChain<Throwable>()
                .validate(() -> checkKeyExists(metadataEntry))
                .getError();
        if (error.isPresent()) {
            command.getPromise().fail(error.get());
            return;
        }

        Queue<PendingCommand> queue = pendingCommands.computeIfAbsent(command.getKey(), k -> new PriorityQueue<>());
        queue.add(new PendingCommand(command, requestCounter++));
        handlePendingCommands(command.getKey());
    }

    private void handlePendingCommands(String key) {
        MetadataEntry metadataEntry = keyMetadataMap.get(key);
        if (metadataEntry == null) {
            return;
        }

        Queue<PendingCommand> queue = pendingCommands.get(key);
        if (queue == null || queue.isEmpty()) {
            return;
        }

        while (!queue.isEmpty()) {
            Command command = queue.peek().command;
            // Check if we can handle the command straight away
            if ((!command.isReadOnly() || metadataEntry.getState() == MetadataEntryState.VALID || command instanceof CreateCommand)
                    || (command.isReadOnly() && command.isAllowInvalidReadsEnabled())) {
                queue.remove();
                coordinateCommand(command);
            } else {
                // Wait until we can handle the command
                return;
            }
        }
    }

    private void coordinateCommand(Command command) {
        MetadataEntry metadataEntry = keyMetadataMap.get(command.getKey());

        if (!command.isReadOnly()) {
            metadataEntry.setState(MetadataEntryState.INVALID);

            Timestamp timestamp = metadataEntry.getTimestamp();
            if (command instanceof DeleteCommand) {
                // We increment the version by 2 so a delete operation racing with another command always has a higher
                // timestamp
                timestamp.setVersion(timestamp.getVersion() + 2);
            } else {
                timestamp.setVersion(timestamp.getVersion() + 1);
            }
            timestamp.setCoordinatorVerticleId(verticleInfo.getId());

            if (command instanceof InvalidationCommand) {
                metadataEntry.setOldMetadata(metadataEntry.getMetadata());
                ((InvalidationCommand) command).setOldMetadata(metadataEntry.getMetadata());
            }
        }

        // Apply the command locally
        Object result;
        try {
            result = command.apply(this);
        } catch (Exception e) {
            command.getPromise().fail(e);
            return;
        }

        if (command.isReadOnly()) {
            command.getPromise().complete(result);
            // Replication of read-only commands is not required
            return;
        }

        // Create a copy of the timestamp as the timestamp in the metadata entry might change during replication
        Timestamp commandTimestamp = new Timestamp(metadataEntry.getTimestamp());

        // If the command is a SynchronizedReconfigureCommand, we set the seen KeyMetadata to null
        // and the new KeyMetadata to the final configuration. This is probably not very beautiful but
        // the simplest way to ensure that the reconfiguration is replicated correctly to all metadata servers.
        if(command instanceof SynchronizedReconfigureCommand){
            ((SynchronizedReconfigureCommand) command).setSeenMetadata(null);
            ((SynchronizedReconfigureCommand) command).setNewMetadata(new KeyMetadata(metadataEntry.getMetadata().getObjectLocations()));
        }

        // Replicate the command
        invalidateReplicas((InvalidationCommand) command, commandTimestamp)
                .compose(res -> {
                    // All metadata replicas acknowledged the invalidation, so we can complete the request promise
                    command.getPromise().complete(result);
                    return commitReplicas(command.getKey(), commandTimestamp);
                })
                .onSuccess(res -> commitIfTimestampIsMostRecent(command.getKey(), metadataEntry, commandTimestamp))
                .onFailure(err ->
                        // In case the request promise has not been completed yet, complete it with the error
                        command.getPromise().tryFail(err)
                );
    }

    private Future<Void> invalidateReplicas(InvalidationCommand command, Timestamp writeTimestamp) {
        Promise<Void> replicaInvalidationPromise = Promise.promise();
        invalidateReplicasWithRetries(command, writeTimestamp, replicaInvalidationPromise, 0);
        return replicaInvalidationPromise.future();
    }

    private void invalidateReplicasWithRetries(InvalidationCommand command, Timestamp writeTimestamp,
                                               Promise<Void> promise, int attempt) {
        MetadataEntry metadataEntry = keyMetadataMap.get(command.getKey());
        Timestamp currentTimestamp = metadataEntry.getTimestamp();
        if (currentTimestamp.isGreaterThan(writeTimestamp)) {
            // We received a concurrent operation with a higher timestamp => abort
            promise.fail(new ConcurrentOperationException());
            return;
        }

        // First send the invalidation to all main metadata verticles and afterwards to the storage verticles of the
        // old and new configuration
        Set<Integer> destinations1 = new HashSet<>(metadataVerticleIds);
        destinations1.remove(verticleInfo.getId());
        sendInvalidations(destinations1, writeTimestamp, command)
                .compose(res -> {
                    Set<Integer> destinations2 = metadataEntry.getMetadata() != null
                            ? new HashSet<>(metadataEntry.getMetadata().getObjectLocations())
                            : new HashSet<>();
                    if (metadataEntry.getOldMetadata() != null) {
                        destinations2.addAll(metadataEntry.getOldMetadata().getObjectLocations());
                    }
                    destinations2.remove(verticleInfo.getId());
                    return sendInvalidations(destinations2, writeTimestamp, command);
                })
                .onSuccess(res -> promise.complete())
                .onFailure(err -> {
                    logger.debug("Failed to invalidate metadata replicas of key " + command.getKey(), err);
                    if(attempt < MAX_RETRIES) {
                        // Wait a bit and retry
                        vertx.setTimer(1000, timerId -> invalidateReplicasWithRetries(command, writeTimestamp, promise, attempt + 1));
                    }else{
                        promise.fail(err);
                    }
                });
    }

    @SuppressWarnings("rawtypes")
    private CompositeFuture sendInvalidations(Set<Integer> destinations, Timestamp writeTimestamp,
                                              InvalidationCommand command) {
        List<Future> rpcFutures = new ArrayList<>();
        for (int verticleId : destinations) {
            rpcFutures.add(metadataRpcService.invalidate(verticleId, membershipView.getEpoch(), writeTimestamp, command));
        }
        return CompositeFuture.join(rpcFutures);
    }

    private void commitIfTimestampIsMostRecent(String key, MetadataEntry metadataEntry, Timestamp timestamp) {
        if (!timestamp.equals(metadataEntry.getTimestamp())) {
            return;
        }

        metadataEntry.setOldMetadata(null);
        metadataEntry.setState(MetadataEntryState.VALID);
        if (listener != null) {
            listener.keyValidated(key, metadataEntry.getMetadata());
        }

        // Check if we are still responsible for this key
        if (metadataEntry.getMetadata().getObjectLocations().isEmpty()
                || (verticleInfo.getType() == VerticleType.STORAGE
                && !metadataEntry.getMetadata().getObjectLocations().contains(verticleInfo.getId()))) {
            // Reject all pending commands and delete the key
            Queue<PendingCommand> queue = pendingCommands.remove(key);
            if (queue != null) {
                queue.forEach(pending -> pending.command.getPromise().fail(new KeyDoesNotExistException()));
            }
            keyMetadataMap.remove(key);
        } else {
            vertx.runOnContext(v -> handlePendingCommands(key));
        }
    }

    private Future<Void> commitReplicas(String key, Timestamp writeTimestamp) {
        Promise<Void> replicaValidationPromise = Promise.promise();
        commitReplicasWithRetries(key, writeTimestamp, replicaValidationPromise, 0);
        return replicaValidationPromise.future();
    }

    private void commitReplicasWithRetries(String key, Timestamp writeTimestamp, Promise<Void> promise, int attempt) {
        MetadataEntry metadataEntry = keyMetadataMap.get(key);
        Set<Integer> metadataLocations = getMetadataReplicaLocations(metadataEntry);

        @SuppressWarnings("rawtypes")
        List<Future> rpcFutures = new ArrayList<>();
        for (Integer location : metadataLocations) {
            rpcFutures.add(metadataRpcService.commit(location, membershipView.getEpoch(), key, writeTimestamp));
        }
        CompositeFuture.join(rpcFutures)
                .onSuccess(res -> promise.complete())
                .onFailure(err -> {
                    logger.debug("Failed to validate metadata replicas of object with key " + key, err);
                    if(attempt < MAX_RETRIES) {
                        // Wait a bit and retry
                        vertx.setTimer(1000, timerId -> commitReplicasWithRetries(key, writeTimestamp, promise, attempt + 1));
                    }else{
                        promise.fail(err);
                    }
                });
    }

    private Set<Integer> getMetadataReplicaLocations(MetadataEntry metadataEntry) {
        Set<Integer> locations = new HashSet<>(metadataVerticleIds);
        locations.addAll(metadataEntry.getMetadata().getObjectLocations());
        if (metadataEntry.getOldMetadata() != null) {
            locations.addAll(metadataEntry.getOldMetadata().getObjectLocations());
        }
        locations.remove(verticleInfo.getId());
        return locations;
    }

    private KeyConfiguration getConfigurationForKey(MetadataEntry metadataEntry) {
        if (metadataEntry.getMetadata() == null) {
            return null;
        }
        List<Storage> storages = metadataEntry.getMetadata().getObjectLocations().stream()
                .map(verticleId -> {
                    VerticleInfo verticle = membershipView.findVerticleById(verticleId);
                    return new Storage(
                            verticle.getId(),
                            verticle.getOwnerNode().getRegion(),
                            verticle.getOwnerNode().getHostname(),
                            verticle.getPort()
                    );
                })
                .collect(Collectors.toList());
        return new KeyConfiguration((int) metadataEntry.getTimestamp().getVersion(), storages);
    }

    private void onMembershipChange(MembershipView membershipView) {
        if (membershipView == null ||
                (this.membershipView != null && membershipView.getEpoch() <= this.membershipView.getEpoch())) {
            return;
        }
        MembershipView oldMembershipView = this.membershipView;
        this.membershipView = membershipView;
        metadataVerticleIds = membershipView.getVerticleIdsByType(VerticleType.METADATA);
        storageVerticleIdsPerZone = membershipView.getStorageVerticleIdsByZone();
        metadataVerticleIdsPerZone = membershipView.getMetadataVerticleIdsByZone();
        if (oldMembershipView == null) {
            return;
        }
        Set<Integer> oldStorageVerticles = oldMembershipView.getVerticleIdsByType(VerticleType.STORAGE);
        Set<Integer> storageVerticleIds = membershipView.getVerticleIdsByType(VerticleType.STORAGE);
        Set<Integer> lostStorageVerticles = new HashSet<>(oldStorageVerticles);
        Set<Integer> newStorageVerticles = new HashSet<>(storageVerticleIds);;
        lostStorageVerticles.removeAll(storageVerticleIds);
        newStorageVerticles.removeAll(oldStorageVerticles);

        // If new storage verticles occured, we have to copy all client function extensions to these new verticles!
        if(!newStorageVerticles.isEmpty()){
            keyMetadataMap.entrySet().stream().filter(entry -> entry.getValue().getMetadata().isFullReplication())
                    .forEach(entry ->{
                entry.getValue().getMetadata().getObjectLocations().addAll(newStorageVerticles);
            });
        }

        if (lostStorageVerticles.isEmpty()) {
            return;
        }
        for (Map.Entry<String, MetadataEntry> entry : keyMetadataMap.entrySet()) {
            MetadataEntry metadataEntry = entry.getValue();
            boolean ownerLeft = false;
            if (!metadataEntry.getMetadata().getObjectLocations().isEmpty()) {
                Integer currentOwner = metadataEntry.getMetadata().getObjectLocations().iterator().next();
                if (lostStorageVerticles.contains(currentOwner)) {
                    ownerLeft = true;
                }
            }
            metadataEntry.getMetadata().getObjectLocations().removeAll(lostStorageVerticles);
            if (metadataEntry.getOldMetadata() != null) {
                metadataEntry.getOldMetadata().getObjectLocations().removeAll(lostStorageVerticles);
            }
            if (ownerLeft && listener != null) {
                listener.keyOwnerLeft(entry.getKey(), metadataEntry.getMetadata());
            }
        }
    }

    private CommandRejectedException checkOriginEpoch(int originEpoch) {
        return originEpoch != membershipView.getEpoch()
                ? new CommandRejectedException("Origin epoch does not match local epoch")
                : null;
    }

    private KeyDoesNotExistException checkKeyExists(MetadataEntry metadataEntry) {
        return metadataEntry == null ? new KeyDoesNotExistException() : null;
    }

    protected enum CommandPriorityGroup {
        // Priorities need to be ordered from highest to lowest

        HIGH,

        NORMAL
    }

    protected static class PendingCommand implements Comparable<PendingCommand> {

        protected final Command command;

        protected final int requestId;

        public PendingCommand(Command command, int requestId) {
            this.command = command;
            this.requestId = requestId;
        }

        @Override
        public int compareTo(PendingCommand other) {
            CommandPriorityGroup priorityGroup = getPriorityGroup(command);
            CommandPriorityGroup otherPriorityGroup = getPriorityGroup(other.command);

            if (priorityGroup.compareTo(otherPriorityGroup) < 0) {
                // This command has a higher priority than the other
                return -1;
            } else if (priorityGroup.compareTo(otherPriorityGroup) > 0) {
                // This command has a lower priority than the other
                return 1;
            } else {
                // Both commands are in the same priority group, we use the request ID as a tiebreaker (the command
                // with the lower request ID has a higher priority)
                return Integer.compare(requestId, other.requestId);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PendingCommand that = (PendingCommand) o;
            return requestId == that.requestId && command.equals(that.command);
        }

        @Override
        public int hashCode() {
            return Objects.hash(requestId, command);
        }

        protected CommandPriorityGroup getPriorityGroup(Command command) {
            return command.isAllowInvalidReadsEnabled()
                    ? CommandPriorityGroup.HIGH
                    : CommandPriorityGroup.NORMAL;
        }
    }
}
