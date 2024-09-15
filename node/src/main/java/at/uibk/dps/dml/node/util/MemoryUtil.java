package at.uibk.dps.dml.node.util;

import org.openjdk.jol.info.GraphLayout;

public class MemoryUtil {

    /***
     * Returns a rough estimate of the deep size of the object in bytes in memory
     * @param object the object whose deep size should be evaluated
     * @return an estimate of the deep size of the object in bytes
     */
    public static long getDeepObjectSize(Object object){
        return GraphLayout.parseInstance(object).totalSize();
    }

}
