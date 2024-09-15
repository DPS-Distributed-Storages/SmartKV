package at.uibk.dps.dml.node.statistics;

import at.uibk.dps.dml.node.billing.TCPRequestType;

import java.util.Arrays;
import java.util.EnumMap;

public class StatisticEntry {

   private final EnumMap<TCPRequestType, Long> numberOfRequests;

   // TODO: Should we differentiate between ingress and egress or not?
   private final EnumMap<TCPRequestType, Long> cumulativeMessageSizeReceived;

   private final EnumMap<TCPRequestType, Long> cumulativeMessageSizeSent;

    public StatisticEntry() {
       numberOfRequests = new EnumMap<>(TCPRequestType.class);
       Arrays.stream(TCPRequestType.values()).forEach(type -> this.numberOfRequests.put(type, 0L));

       cumulativeMessageSizeReceived = new EnumMap<>(TCPRequestType.class);
       Arrays.stream(TCPRequestType.values()).forEach(type -> this.cumulativeMessageSizeReceived.put(type, 0L));

       cumulativeMessageSizeSent = new EnumMap<>(TCPRequestType.class);
       Arrays.stream(TCPRequestType.values()).forEach(type -> this.cumulativeMessageSizeSent.put(type, 0L));
    }

    public void addRequest(TCPRequestType requestType){
      long requests = numberOfRequests.get(requestType);
      numberOfRequests.put(requestType, requests + 1);
   }

   public void addReceivedMessageSize(TCPRequestType requestType, long messageSizeInBytes){
      long cumulativeMessageSize = cumulativeMessageSizeReceived.get(requestType);
      cumulativeMessageSizeReceived.put(requestType, cumulativeMessageSize + messageSizeInBytes);
   }

   public void addSentMessageSize(TCPRequestType requestType, long messageSizeInBytes){
      long cumulativeMessageSize = cumulativeMessageSizeSent.get(requestType);
      cumulativeMessageSizeSent.put(requestType, cumulativeMessageSize + messageSizeInBytes);
   }

   public EnumMap<TCPRequestType, Long> getNumberOfRequests() {
      return numberOfRequests;
   }

   public EnumMap<TCPRequestType, Long> getCumulativeMessageSizeReceived() {
      return cumulativeMessageSizeReceived;
   }

   public EnumMap<TCPRequestType, Long> getCumulativeMessageSizeSent() {
      return cumulativeMessageSizeSent;
   }

    public void resetToZero(){
        Arrays.stream(TCPRequestType.values()).forEach(type -> {
            numberOfRequests.put(type, 0L);
            cumulativeMessageSizeReceived.put(type, 0L);
            cumulativeMessageSizeSent.put(type, 0L);
        });
    }

}
