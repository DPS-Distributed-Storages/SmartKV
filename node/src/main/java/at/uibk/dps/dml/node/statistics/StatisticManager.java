package at.uibk.dps.dml.node.statistics;


import at.uibk.dps.dml.client.storage.BsonArgsCodec;
import at.uibk.dps.dml.node.billing.TCPRequestType;
import at.uibk.dps.dml.node.statistics.command.*;
import at.uibk.dps.dml.node.storage.StorageService;
import io.vertx.core.Future;
import io.vertx.core.Promise;

import java.util.*;

public class StatisticManager implements CommandHandler {

    // Interval in seconds after which statistics will be flushed to the records
    private final int flushIntervalSeconds;

    private final int numberOfRecords;

    private int currentRecord;

    private long lastFlush;

    private Map<StatisticKey, StatisticEntry> statistics;

    private final Record[] records;

    private final StorageService storageService;

    private final BsonArgsCodec argsCodec;

    public StatisticManager(StorageService storageService) {
        this(storageService,  Integer.parseInt(System.getenv().getOrDefault("STATISTICS_FLUSH_INTERVAL", "5")), Integer.parseInt(System.getenv().getOrDefault("STATISTICS_NUM_RECORDS", "20")));
    }

    public StatisticManager(StorageService storageService, int flushIntervalSeconds, int numberOfRecords) {
        this.statistics = new HashMap<>();
        this.storageService = storageService;
        this.flushIntervalSeconds = flushIntervalSeconds;
        this.numberOfRecords = numberOfRecords;
        this.currentRecord = 0;
        this.lastFlush = System.currentTimeMillis();
        this.argsCodec = new BsonArgsCodec();
        this.records = new Record[numberOfRecords];
        for(int i = 0; i < records.length; i++){
            records[i] = new Record(-i-1, new HashMap<>());
        }
    }

    private void checkFlush(){
        long now = System.currentTimeMillis();
        if(((now - lastFlush) / 1000) > flushIntervalSeconds)
            flush(now);
    }

    private void flush(long timestamp){
        // update last flush timestamp
        lastFlush = System.currentTimeMillis();
        // write the record
        if(!statistics.isEmpty()){
            if(currentRecord >= numberOfRecords)
                currentRecord = 0;
            records[currentRecord] = new Record(timestamp, statistics);
            // reset the statistics
            this.statistics = new HashMap<>();
            ++currentRecord;
        }
    }

    public void addStatistic(TCPRequestType requestType, long receivedMessageSizeInBytes, long sentMessageSizeInBytes, StatisticKey statisticKey){
        addRequest(requestType, statisticKey);
        addSentMessageSize(requestType, sentMessageSizeInBytes, statisticKey);
        addReceivedMessageSize(requestType, receivedMessageSizeInBytes, statisticKey);
        checkFlush();
    }

    public void addStatistic(TCPRequestType requestType, long receivedMessageSizeInBytes, long sentMessageSizeInBytes, String key, String clientZone){
        addStatistic(requestType, receivedMessageSizeInBytes, sentMessageSizeInBytes, new StatisticKey(key, clientZone));
    }

    private void addRequest(TCPRequestType requestType, String key, String clientZone){
        addRequest(requestType, new StatisticKey(key, clientZone));
    }

    private void addRequest(TCPRequestType requestType, StatisticKey statisticKey){
            if (!statistics.containsKey(statisticKey)) {
                statistics.put(statisticKey, new StatisticEntry());
            }
            StatisticEntry statisticEntry = statistics.get(statisticKey);
            statisticEntry.addRequest(requestType);
    }

    private void addReceivedMessageSize(TCPRequestType requestType, long messageSizeInBytes, String key, String clientZone){
        addReceivedMessageSize(requestType, messageSizeInBytes, new StatisticKey(key, clientZone));
    }

    private void addReceivedMessageSize(TCPRequestType requestType, long messageSizeInBytes, StatisticKey statisticKey){
        if(!statistics.containsKey(statisticKey)){
            statistics.put(statisticKey, new StatisticEntry());
        }
        StatisticEntry statisticEntry = statistics.get(statisticKey);
        statisticEntry.addReceivedMessageSize(requestType, messageSizeInBytes);
    }

    private void addSentMessageSize(TCPRequestType requestType, long messageSizeInBytes, String key, String clientZone){
        addSentMessageSize(requestType, messageSizeInBytes, new StatisticKey(key, clientZone));
    }

    private void addSentMessageSize(TCPRequestType requestType, long messageSizeInBytes, StatisticKey statisticKey){
        if(!statistics.containsKey(statisticKey)){
            statistics.put(statisticKey, new StatisticEntry());
        }
        StatisticEntry statisticEntry = statistics.get(statisticKey);
        statisticEntry.addSentMessageSize(requestType, messageSizeInBytes);
    }

    public void resetAllToZero(){
        this.statistics.clear();
        for(int i = 0; i < records.length; i++){
            records[i] = new Record(-i-1, new HashMap<>());
        }
    }

    public Map<StatisticKey, StatisticEntry> getStatistics() {
        return statistics;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Future<Record[]> provideStatistics() {
        Promise promise = Promise.promise();
        coordinateCommand(new GetStatisticsCommand(promise));
        return promise.future();
    }

    public Future<Void> clearStatistics() {
        Promise promise = Promise.promise();
        coordinateCommand(new ClearStatisticsCommand(promise));
        return promise.future();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Future<Map<String, Set<Integer>>> getMainObjectLocations() {
        Promise promise = Promise.promise();
        coordinateCommand(new GetMainObjectLocationsCommand(promise));
        return promise.future();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Future<byte[]> getPrices() {
        Promise promise = Promise.promise();
        coordinateCommand(new GetUnitPricesCommand(promise));
        return promise.future();
    }

    public Future<byte[]> getBills() {
        Promise promise = Promise.promise();
        coordinateCommand(new GetBillsCommand(promise));
        return promise.future();
    }

    public Future<Long> getFreeMemory() {
        Promise promise = Promise.promise();
        coordinateCommand(new GetFreeMemoryCommand(promise));
        return promise.future();
    }

    private void coordinateCommand(Command command) {
        // Apply the command
        Object result;
        try {
            result = command.apply(this);
        } catch (Exception e) {
            command.getPromise().fail(e);
            return;
        }
        command.getPromise().complete(result);
    }

    @Override
    public Object apply(GetStatisticsCommand command) {
        return this.records;
    }

    @Override
    public Object apply(GetMainObjectLocationsCommand getMainObjectLocationsCommand) {
        return storageService.getMainObjectLocations();
    }

    @Override
    public Object apply(GetUnitPricesCommand getUnitPricesCommand) {
        return argsCodec.encode(new Object[]{storageService.getPrices()});
    }

    @Override
    public Object apply(GetBillsCommand getBillsCommand) {
        return argsCodec.encode(new Object[]{storageService.getBills()});
    }

    @Override
    public Object apply(ClearStatisticsCommand clearStatisticsCommand) {
        this.resetAllToZero();
        return null;
    }

    @Override
    public Object apply(GetFreeMemoryCommand getFreeMemoryCommand) {
        return storageService.getFreeMemory();
    }
}
