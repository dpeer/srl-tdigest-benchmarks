package SrlTDigestBenchmarks.TransDigest;

import Common.DbConn;
import Common.LgData;
import Common.SrlConsts;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;


public class TransAggsPerformanceBench {
    private static Random gen = new Random();

    static double[][] generateLgData(int aggsNum) {
        // Create random data
        //System.out.println("Generating " + SrlConsts.MaxDimensionsPerLg * SrlConsts.LgValuesPerDimension + " random values grouped by " + SrlConsts.MaxDimensionsPerLg + " dimensions per LGs");
        var data = new double[SrlConsts.MaxDimensions / aggsNum][SrlConsts.LgValuesPerDimension];

        for (int i = 0; i < SrlConsts.MaxDimensions / aggsNum; i++) {
            for (int j = 0; j< SrlConsts.LgValuesPerDimension; j++){
                data[i][j] = gen.nextDouble() * 100;
            }
        }

        return data;
    }

    public static void main(String[] args) throws SQLException {
        if (args.length != 3) {
            System.out.println("Usage: aggregatorNum ThreadsNum saveToDB");
            System.exit(-1);
        }

        int aggsNum = Integer.parseInt(args[0]);
        int threadsNum = Integer.parseInt(args[1]);
        boolean saveToDB = Boolean.parseBoolean(args[2]);
        var lgsData = new ArrayList<LgData>(SrlConsts.MaxLgs);

        if (saveToDB) {
            DbConn.init();
        }

        long totalStartDur = System.currentTimeMillis();

        for (int iterNum = 0; iterNum < SrlConsts.NumIterations; iterNum++) {
           System.out.println("******** Iteration #" + (iterNum + 1) + "/" + SrlConsts.NumIterations + " ********");

           lgsData.clear();
           // Per LG:
           // 1. Generate random data
           // 2. Run TDigest
           // 3. Convert to ByteBuffer
           for (int i = 0; i < (SrlConsts.MaxLgs); i++) {
               System.out.println("LG #" + (i + 1) + "/" + SrlConsts.MaxLgs);
               var data = generateLgData(aggsNum);
               var lgData = new LgData(i % SrlConsts.MaxRegions, data, SrlConsts.MaxEmulations, SrlConsts.MaxTransactions / aggsNum);
               lgData.createTransTDigests();
               lgsData.add(lgData);

               if ((i % 100) == 0) {
                   var sumSize = 0;
                   for (var transTDigestsBuffers : lgData.getTransTDigestsBuffers().values()) {
                       sumSize += transTDigestsBuffers.position();
                   }
                   System.out.println("LG total buffers size = " + sumSize);
               }
           }

           var transNames = new HashSet<String>();

           // Reset buffers
           for (var lgData : lgsData) {
               transNames.addAll(lgData.getTransNames());
               lgData.rewindTransBuffers();
           }

           System.out.println("#Transactions from LGs = " + transNames.size());

           var resAgg = new TransResAgg(
                   "1",
                   iterNum * SrlConsts.AggDurationMilliSeconds,
                   (iterNum + 1) * SrlConsts.AggDurationMilliSeconds,
                   lgsData,
                   transNames.size(),
                   threadsNum);
           resAgg.createTDigests(saveToDB);
       }

        long totalEndDur = System.currentTimeMillis();
        System.out.println("Main: total duration (msec) = " + ((totalEndDur - totalStartDur)));
    }
}
