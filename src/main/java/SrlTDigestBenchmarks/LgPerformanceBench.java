package SrlTDigestBenchmarks;

import java.util.ArrayList;
import java.util.Random;


public class LgPerformanceBench {
    private static Random gen = new Random();
    private static long startDur;
    private static long endDur;
    private static final int threadsCount = 25;

    static double[][] generateLgData() {
        // Create random data
        System.out.println("Generating " + SrlConsts.MaxDimensionsPerLg * SrlConsts.LgValuesPerDimension + " random values grouped by " + SrlConsts.MaxDimensionsPerLg + " dimensions per LGs");
        var data = new double[SrlConsts.MaxDimensionsPerLg][SrlConsts.LgValuesPerDimension];

        for (int i = 0; i < SrlConsts.MaxDimensionsPerLg; i++) {
            for (int j = 0; j< SrlConsts.LgValuesPerDimension; j++){
                data[i][j] = gen.nextDouble() * 100;
            }
        }

        return data;
    }

    public static void main(String[] args) {
        //var lgsBuffers = new ByteBuffer[SrlConsts.TDigestsPerResultsAggregator][];
        var lgsData = new ArrayList<LgData>(SrlConsts.TDigestsPerResultsAggregator);

        // Per LG:
        // 1. Generate random data
        // 2. Run TDigest
        // 3. Convert to ByteBuffer
        for (int i = 0; i < (SrlConsts.TDigestsPerResultsAggregator); i++) {
            System.out.println("LG #" + (i + 1) + "/" + SrlConsts.TDigestsPerResultsAggregator);
            var data = generateLgData();
            var lgData = new LgData(i % SrlConsts.MaxRegions, data);
            lgData.createTDigests();
            lgsData.add(lgData);
        }

        // Reset buffers
        for (var lgData : lgsData) {
            lgData.rewindBuffers();
        }

        var resAgg = new ResAgg(lgsData);
        try {
            resAgg.createTDigests();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//        try {
//            mergeLgsTDigestsMultiT(lgsData);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        // todo: Add processing of all Aggs and reporter
    }
}
