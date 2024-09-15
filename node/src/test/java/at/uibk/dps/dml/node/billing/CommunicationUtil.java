package at.uibk.dps.dml.node.billing;

public class CommunicationUtil {

    protected static boolean communicationsAreReset(CommunicationMetrics communicationMetrics){
        for(Object value : communicationMetrics.getNumberOfRequests().values()){
            if (!value.equals(0L))
                return false;
        }
        for(Object value : communicationMetrics.getDataTransferIn().values()){
            if (!value.equals(0L))
                return false;
        }
        for(Object value : communicationMetrics.getDataTransferOut().values()){
            if (!value.equals(0L))
                return false;
        }
        return true;
    }

}
