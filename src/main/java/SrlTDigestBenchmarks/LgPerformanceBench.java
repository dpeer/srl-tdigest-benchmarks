package SrlTDigestBenchmarks;

import java.util.Random;


public class LgPerformanceBench {
    private static Random gen = new Random();
    private static long startDur;
    private static long endDur;

    static double[][] generateLgData() {
        // Create random data
        //System.out.println("Generating " + SrlConsts.MaxDimensionsPerLg * SrlConsts.LgValuesPerDimension + " random values grouped by " + SrlConsts.MaxDimensionsPerLg + " dimensions per LGs");
        var data = new double[SrlConsts.MaxDimensionsPerLg][SrlConsts.LgValuesPerDimension];

        for (int i = 0; i < SrlConsts.MaxDimensionsPerLg; i++) {
            for (int j = 0; j< SrlConsts.LgValuesPerDimension; j++){
                data[i][j] = gen.nextDouble() * 100;
            }
        }

        return data;
    }

    public static void main(String[] args) {
        var data = generateLgData();
        var lgData = new LgData(0, data);

        System.out.println("Creating LG TDigests");
        startDur = System.currentTimeMillis();
        lgData.createTDigests();
        //lgData.rewindBuffers();
        endDur = System.currentTimeMillis();
        System.out.println("Creating LG TDigests duration (msec) = " + (endDur - startDur));

        var sumSize = 0;
        for (var lgDimensionsData : lgData.getLgDimensionsData()) {
            sumSize += lgDimensionsData.getTDigestBuffer().position();
        }
        System.out.println("Total buffers size = " + sumSize);
    }
}
