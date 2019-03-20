package SrlTDigestBenchmarks.TransDigest;

import Common.*;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class TransAggsPerformanceBench {
    private static Random gen = new Random();

    static double[][] generateLgData() {
        // Create random data
        var data = new double[Config.getInstance().getMaxDimensions() / Config.getInstance().getAggsNum()][Config.getInstance().getLgValuesPerDimension()];

        for (int i = 0; i < Config.getInstance().getMaxDimensions() / Config.getInstance().getAggsNum(); i++) {
            for (int j = 0; j< Config.getInstance().getLgValuesPerDimension(); j++){
                data[i][j] = gen.nextDouble() * 100;
            }
        }

        return data;
    }

    public static void main(String[] args) throws SQLException {
        long totalRawDataCount = 0;
        long totalLgsTdigestsSize = 0;
        var lgsData = new ArrayList<LgData>(Config.getInstance().getMaxLgs());
        long totalStartDur = System.currentTimeMillis();
        long startDur, endDur;

        if (Config.getInstance().isSaveToDB()) {
            if (!DbConn.init()) {
                System.out.println("Failed to connect to DB");
                System.exit(-1);
            }
        }

        if (Config.getInstance().isFromSrcDB()) {
            if (!DbConnSrc.init()) {
                System.out.println("Failed to connect to Source DB");
                System.exit(-1);
            }
        }

        if (Config.getInstance().isFromSrcDB()) {
            var interNum = 0;
            while (true) {
                System.out.println("******** Interval #" + (interNum + 1) + " ********");
                lgsData.clear();
                startDur = System.currentTimeMillis();
                var rawData = DalSrc.getRawData(
                        Config.getInstance().getTestId(),
                        Config.getInstance().getRunId(),
                        interNum * Config.getInstance().getAggDurationMilliSeconds(),
                        ((interNum + 1) * Config.getInstance().getAggDurationMilliSeconds()) - 1);
                var rawDataCount = rawData.size();
                endDur = System.currentTimeMillis();
                System.out.println("getRawData duration (msec) = " + ((endDur - startDur)) + "; Raw data count = " + rawDataCount);
                if (rawDataCount == 0) {
                    break;
                }
                totalRawDataCount += rawDataCount;

                AtomicInteger counter = new AtomicInteger(0);
                var rawDataPartitions = rawData.stream()
                        .collect(Collectors.groupingBy(it -> counter.getAndIncrement() % Config.getInstance().getMaxLgs()))
                        .values();

                for (var rawDataPart : rawDataPartitions) {
                    var lgData = new LgData(1, rawDataPart);
                    lgData.createTransTDigests();
                    lgsData.add(lgData);

                    var lgTdigestsSize = 0;
                    for (var transTDigestsBuffers : lgData.getTransTDigestsBuffers().values()) {
                        lgTdigestsSize += transTDigestsBuffers.position();
                    }
                    System.out.println("LG total buffers size (bytes) = " + lgTdigestsSize);
                    totalLgsTdigestsSize += lgTdigestsSize;
                }

                var transNames = new HashSet<String>();

                // Reset buffers
                for (var lgData : lgsData) {
                    transNames.addAll(lgData.getTransNames());
                    lgData.rewindTransBuffers();
                }

                System.out.println("#Transactions from LGs = " + transNames.size());

                var resAgg = new TransResAgg(
                        interNum * Config.getInstance().getAggDurationMilliSeconds(),
                        ((interNum + 1) * Config.getInstance().getAggDurationMilliSeconds()) - 1,
                        lgsData,
                        transNames.size(),
                        Config.getInstance().getThreadsNum());
                resAgg.createTDigests(Config.getInstance().isSaveToDB());

                interNum++;
            }
        } else {
            for (int iterNum = 0; iterNum < Config.getInstance().getNumIterations(); iterNum++) {
                System.out.println("******** Iteration #" + (iterNum + 1) + "/" + Config.getInstance().getNumIterations() + " ********");
                lgsData.clear();
                // Per LG:
                // 1. Generate random data
                // 2. Run TDigest
                // 3. Convert to ByteBuffer
                for (int i = 0; i < Config.getInstance().getMaxLgs(); i++) {
                    System.out.println("LG #" + (i + 1) + "/" + Config.getInstance().getMaxLgs());
                    var rawData = generateLgData();
                    totalRawDataCount += (Config.getInstance().getMaxDimensions() / Config.getInstance().getAggsNum()) * Config.getInstance().getLgValuesPerDimension();
                    var lgData = new LgData(
                            i % Config.getInstance().getMaxRegions(),
                            rawData,
                            Config.getInstance().getMaxEmulations(),
                            Config.getInstance().getMaxTransactions() / Config.getInstance().getAggsNum());
                    lgData.createTransTDigests();
                    lgsData.add(lgData);

                    var lgTdigestsSize = 0;
                    for (var transTDigestsBuffers : lgData.getTransTDigestsBuffers().values()) {
                        lgTdigestsSize += transTDigestsBuffers.position();
                    }
                    System.out.println("LG total buffers size (bytes) = " + lgTdigestsSize);
                    totalLgsTdigestsSize += lgTdigestsSize;
                }

                var transNames = new HashSet<String>();

                // Reset buffers
                for (var lgData : lgsData) {
                    transNames.addAll(lgData.getTransNames());
                    lgData.rewindTransBuffers();
                }

                System.out.println("#Transactions from LGs = " + transNames.size());

                var resAgg = new TransResAgg(
                        iterNum * Config.getInstance().getAggDurationMilliSeconds(),
                        ((iterNum + 1) * Config.getInstance().getAggDurationMilliSeconds()) - 1,
                        lgsData,
                        transNames.size(),
                        Config.getInstance().getThreadsNum());
                resAgg.createTDigests(Config.getInstance().isSaveToDB());
            }
        }

        long totalEndDur = System.currentTimeMillis();
        System.out.println("Total duration (msec) = " + (totalEndDur - totalStartDur));

        System.out.println("Total raw data (count) = " + totalRawDataCount);
        System.out.println("Total LGs tdigest size (bytes) = " + totalLgsTdigestsSize);
    }
}
