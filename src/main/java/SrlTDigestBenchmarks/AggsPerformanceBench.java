package SrlTDigestBenchmarks;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;


public class AggsPerformanceBench {
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
        if (args.length != 2) {
            System.out.println("Usage: aggrgatorNum ThreadsNum");
            System.exit(-1);
        }
        int aggsNum = Integer.parseInt(args[0]);
        int threadsNum = Integer.parseInt(args[1]);
        var lgsData = new ArrayList<LgData>(SrlConsts.MaxLgs);

        // Per LG:
        // 1. Generate random data
        // 2. Run TDigest
        // 3. Convert to ByteBuffer
        for (int i = 0; i < (SrlConsts.MaxLgs); i++) {
            System.out.println("LG #" + (i + 1) + "/" + SrlConsts.MaxLgs);
            var data = generateLgData();
            var lgData = new LgData(i % SrlConsts.MaxRegions, data, SrlConsts.MaxEmulations, SrlConsts.MaxTransactions / aggsNum);
            lgData.createTDigests();
            lgsData.add(lgData);
        }

        var dimensionsIds = new HashSet<String>();

        // Reset buffers
        for (var lgData : lgsData) {
            dimensionsIds.addAll(lgData.getDimensionsIds());
            lgData.rewindBuffers();
        }

        System.out.println("#Dimensions from LGs = " + dimensionsIds.size());

        var resAgg = new ResAgg(lgsData, dimensionsIds.size(), threadsNum);
        try {
            resAgg.createTDigests();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
