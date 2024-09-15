package at.uibk.dps.dml.client.storage.object.extensions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.OptionalDouble;

/**
 * The {@link SharedStatistics} holds shared statistics.
 */
public class SharedStatistics implements Serializable {

    private ArrayList<Long> statistics;

    /**
     * Default constructor that initializes the statistics with an empty ArrayList
     */
    public SharedStatistics() {
        statistics = new ArrayList<>();
    }


    /**
     * Adds a value to the statistics.
     *
     * @param value the value to be added
     * @return true if the value was added to the statistics
     */
    public Boolean add(long value) {
        return statistics.add(value);
    }

    /**
     * Removes the first occurrence of the specified element from the statistics
     * @param value the value to remove
     * @return true if this list contained the specified element
     */
    public Boolean remove(long value){
        return statistics.remove(value);
    }

    /**
     * Calculates and returns an average over all statistic elements
     * @return the average
     */
    public Double average(){
        OptionalDouble average = statistics.stream().mapToDouble(a->a).average();
        return average.isPresent() ? average.getAsDouble() : 0.0;
    }

    /**
     * Returns the current values of the statistic.
     *
     * @return the current values of the statistics
     */
    public ArrayList<Long> getStatistics() {
        return statistics;
    }

    public void setStatistics(ArrayList<Long> statistics) {
        this.statistics = statistics;
    }
}
