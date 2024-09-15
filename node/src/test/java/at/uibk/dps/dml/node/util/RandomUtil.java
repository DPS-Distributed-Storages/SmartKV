package at.uibk.dps.dml.node.util;

import io.netty.util.internal.ThreadLocalRandom;

import java.util.Arrays;
import java.util.Random;

public class RandomUtil {

    public static long sumLongValues(long[] values){
        return Arrays.stream(values).sum();
    }

    public static long generateRandomLongValue(long min, long max){
        return ThreadLocalRandom.current().nextLong(min, max);
    }

    public static int generateRandomIntegerValue(int min, int max){
        return ThreadLocalRandom.current().nextInt(min, max);
    }

    protected static double generateRandomDoubleValue(double min, double max){
        return ThreadLocalRandom.current().nextDouble(min, max);
    }

    public static long generateRandomLongValue(){
        return generateRandomLongValue(1, 25);
    }

    public static long[] generateRandomLongValues(long size){
        return new Random().longs(size, 1, 1024 * 1024).map(Math::abs).toArray();
    }

    public static long[] generateRandomLongValues(){
        return generateRandomLongValues(10);
    }

}
