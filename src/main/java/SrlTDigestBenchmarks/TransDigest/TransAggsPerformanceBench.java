package SrlTDigestBenchmarks.TransDigest;

import Common.*;
import Common.Pojos.TransDimensionPojo;

import java.sql.SQLException;
import java.util.*;


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
        if (args.length < 4) {
            System.out.println("Usage: aggregatorNum ThreadsNum saveToDB [fromSrcDB [testId runId]]");
            System.exit(-1);
        }

        int aggsNum = Integer.parseInt(args[0]);
        int threadsNum = Integer.parseInt(args[1]);
        boolean saveToDB = Boolean.parseBoolean(args[2]);
        boolean fromSrcDB = Boolean.parseBoolean(args[3]);
        String testId = "";
        String runId = "";
        if (fromSrcDB) {
            if (args.length < 6) {
                System.out.println("Must provide testId & runId");
                System.exit(-1);
            }
            testId = args[4];
            runId = args[5];
        }
        List<TransDimensionPojo> externalDimensions;
        Map<String, TransDimensionPojo> externalDimensionIds = new HashMap<>();
        long totalRawDataCount = 0;
        long totalLgsTdigestsSize = 0;


        var lgsData = new ArrayList<LgData>(SrlConsts.MaxLgs);

        if (saveToDB) {
            if (!DbConn.init()) {
                System.out.println("Failed to connect to DB");
                System.exit(-1);
            }
        }

        long totalStartDur = System.currentTimeMillis();
        long startDur, endDur;

        if (fromSrcDB) {
            if (!DbConnSrc.init()) {
                System.out.println("Failed to connect to Source DB");
                System.exit(-1);
            }
        }

        for (int iterNum = 0; iterNum < SrlConsts.NumIterations; iterNum++) {
            System.out.println("******** Iteration #" + (iterNum + 1) + "/" + SrlConsts.NumIterations + " ********");

            lgsData.clear();

            if (fromSrcDB) {
                startDur = System.currentTimeMillis();
                var rawData = DalSrc.getRawData(
                        testId,
                        runId,
                        iterNum * SrlConsts.AggDurationMilliSeconds,
                        ((iterNum + 1) * SrlConsts.AggDurationMilliSeconds) - 1);
                var rawDataCount = rawData.size();
                totalRawDataCount += rawDataCount;
                endDur = System.currentTimeMillis();
                System.out.println("getRawData duration (msec) = " + ((endDur - startDur)) + "; Raw data count = " + rawDataCount);
                var countPerLg = rawDataCount / SrlConsts.MaxLgs;

                for (int i = 0; i < SrlConsts.MaxLgs; i++) {
                    var lgRawData = rawData.subList(countPerLg * i, Math.min(countPerLg * (i + 1), rawDataCount));

                    var lgData = new LgData(1, lgRawData);
                    lgData.createTransTDigests();
                    lgsData.add(lgData);

                    var lgTdigestsSize = 0;
                    for (var transTDigestsBuffers : lgData.getTransTDigestsBuffers().values()) {
                        lgTdigestsSize += transTDigestsBuffers.position();
                    }
                    System.out.println("LG total buffers size (bytes) = " + lgTdigestsSize);
                    totalLgsTdigestsSize += lgTdigestsSize;
                }
            } else {
                // Per LG:
                // 1. Generate random data
                // 2. Run TDigest
                // 3. Convert to ByteBuffer
                for (int i = 0; i < SrlConsts.MaxLgs; i++) {
                    System.out.println("LG #" + (i + 1) + "/" + SrlConsts.MaxLgs);
                    var rawData = generateLgData(aggsNum);
                    totalRawDataCount += (SrlConsts.MaxDimensions / aggsNum) * SrlConsts.LgValuesPerDimension;
                    var lgData = new LgData(i % SrlConsts.MaxRegions, rawData, SrlConsts.MaxEmulations, SrlConsts.MaxTransactions / aggsNum);
                    lgData.createTransTDigests();
                    lgsData.add(lgData);

                    var lgTdigestsSize = 0;
                    for (var transTDigestsBuffers : lgData.getTransTDigestsBuffers().values()) {
                        lgTdigestsSize += transTDigestsBuffers.position();
                    }
                    System.out.println("LG total buffers size (bytes) = " + lgTdigestsSize);
                    totalLgsTdigestsSize += lgTdigestsSize;
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
                   iterNum * SrlConsts.AggDurationMilliSeconds,
                   ((iterNum + 1) * SrlConsts.AggDurationMilliSeconds) - 1,
                   lgsData,
                   transNames.size(),
                   threadsNum);
           resAgg.createTDigests(saveToDB);
       }

        long totalEndDur = System.currentTimeMillis();
        System.out.println("Total duration (msec) = " + ((totalEndDur - totalStartDur)));

        System.out.println("Total raw data (count) = " + totalRawDataCount);
        System.out.println("Total LGs tdigest size (bytes) = " + totalLgsTdigestsSize);
    }
}
