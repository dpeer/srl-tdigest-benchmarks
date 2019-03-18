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
            System.out.println("Usage: aggregatorNum ThreadsNum saveToDB fromSrcDB [tenantId testId runId]");
            System.exit(-1);
        }

        int aggsNum = Integer.parseInt(args[0]);
        int threadsNum = Integer.parseInt(args[1]);
        boolean saveToDB = Boolean.parseBoolean(args[2]);
        boolean fromSrcDB = Boolean.parseBoolean(args[3]);
        String tenantId = "";
        String testId = "";
        String runId = "";
        if (fromSrcDB) {
            tenantId = args[4];
            testId = args[5];
            runId = args[6];
            if (tenantId.length() == 0 || testId.length() == 0 || runId.length() == 0) {
                System.out.println("Must provide tenantId & testId & runId");
                System.exit(-1);
            }
        }
        List<TransDimensionPojo> externalDimensions;
        Map<String, TransDimensionPojo> externalDimensionIds = new HashMap<>();


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

            startDur = System.currentTimeMillis();
            externalDimensions = DalSrc.getDimensions(tenantId, testId, runId);
            endDur = System.currentTimeMillis();
            System.out.println("Main: Get source dimensions duration (msec) = " + ((endDur - startDur)) + "; Dimensions count = " + externalDimensions.size());

            for (var externalDimension : externalDimensions) {
                externalDimensionIds.put(externalDimension.getId(), externalDimension);
            }
        }

        for (int iterNum = 0; iterNum < SrlConsts.NumIterations; iterNum++) {
            System.out.println("******** Iteration #" + (iterNum + 1) + "/" + SrlConsts.NumIterations + " ********");

            lgsData.clear();

            if (fromSrcDB) {
                var rawData = DalSrc.getRawData(
                        tenantId,
                        testId,
                        runId,
                        iterNum * SrlConsts.AggDurationMilliSeconds,
                        ((iterNum + 1) * SrlConsts.AggDurationMilliSeconds) - 1);
                var totalSize = rawData.size();
                var sizePerLg = totalSize / SrlConsts.MaxLgs;

                for (int i = 0; i < SrlConsts.MaxLgs; i++) {
                    var lgRawData = rawData.subList(sizePerLg * i, Math.min(sizePerLg * (i + 1), totalSize));

                    var lgData = new LgData(1, lgRawData);
                    lgData.createTransTDigests();
                    lgsData.add(lgData);

                    if (i == 0) {
                        var sumSize = 0;
                        for (var transTDigestsBuffers : lgData.getTransTDigestsBuffers().values()) {
                            sumSize += transTDigestsBuffers.position();
                        }
                        System.out.println("LG total buffers size = " + sumSize);
                    }
                }
            } else {
                // Per LG:
                // 1. Generate random data
                // 2. Run TDigest
                // 3. Convert to ByteBuffer
                for (int i = 0; i < SrlConsts.MaxLgs; i++) {
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
        System.out.println("Main: total duration (msec) = " + ((totalEndDur - totalStartDur)));
    }
}
