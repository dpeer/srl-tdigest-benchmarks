package SrlTDigestBenchmarks.TransDigest;

import Common.*;
import Common.Pojos.TransDimensionPojo;
import Common.Pojos.TransTDigestMetricsPojo;
import com.tdunning.math.stats.MergingDigest;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryTransTDigestMetrics {
    public static void main(String[] args) throws SQLException {
        long totalStartDur, totalEndDur, startDur, endDur, startMemUsed, endMemUsed;

        List<TransDimensionPojo> externalDimensions;
        Map<String, TransDimensionPojo> externalDimensionIds = new HashMap<>();

        if (!DbConn.init()) {
            System.out.println("Failed to connect to DB");
            System.exit(-1);
        }

        totalStartDur = System.currentTimeMillis();

        if (Config.getInstance().isFromSrcDB()) {
            if (!DbConnSrc.init()) {
                System.out.println("Failed to connect to Source DB");
                System.exit(-1);
            }

            startDur = System.currentTimeMillis();
            externalDimensions = DalSrc.getDimensions(Config.getInstance().getTestId(), Config.getInstance().getRunId());
            endDur = System.currentTimeMillis();
            System.out.println("Get source dimensions duration (msec) = " + ((endDur - startDur)) + "; Dimensions count = " + externalDimensions.size());

            for (var externalDimension : externalDimensions) {
                externalDimensionIds.put(externalDimension.getId(), externalDimension);
            }
        }

        startMemUsed = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        System.out.println("Start memory usage = " + startMemUsed);

        System.out.println("Retrieve TDigestTransMetrics from DB");
        startDur = System.currentTimeMillis();

        var tdigestTransMetrics = Dal.getTDigestTransMetrics();

        endDur = System.currentTimeMillis();
        System.out.println("Retrieve TDigestTransMetrics from DB duration (msec) = " + (endDur - startDur));
        endMemUsed = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        System.out.println("Memory delta from start = " + (endMemUsed - startMemUsed));


        System.out.println("Group data per transaction");
        startDur = System.currentTimeMillis();

        var transGroups = tdigestTransMetrics.stream().collect(Collectors.groupingBy(TransTDigestMetricsPojo::getTransactionId));

        // Todo: Add multi threaded implementation.

        endDur = System.currentTimeMillis();
        System.out.println("Group data per transaction duration (msec) = " + (endDur - startDur) + "; #Transactions = " + transGroups.size());
        endMemUsed = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        System.out.println("Memory delta from start = " + (endMemUsed - startMemUsed));

        System.out.println("Create TDigests and calculate percentiles");
        startDur = System.currentTimeMillis();

        var transPercentiles = new HashMap<String, Double>(transGroups.size());

        for (var transGroupEntry : transGroups.entrySet()) {
            var mergeDigest = new MergingDigest(Config.getInstance().getTdQueryCompression());
            for (var transTDigestMetricsPojo : transGroupEntry.getValue()) {
                mergeDigest.add(MergingDigest.fromBytes(ByteBuffer.wrap(transTDigestMetricsPojo.getTdigestBuf())));
            }
            String percentileKey;
            if (Config.getInstance().isFromSrcDB()) {
                var extDimensionId = externalDimensionIds.get(transGroupEntry.getKey());
                percentileKey = extDimensionId.getTransactionName() + "," + extDimensionId.getScriptId();
            } else {
                percentileKey = transGroupEntry.getKey() + "," + transGroupEntry.getKey();
            }
            transPercentiles.put(percentileKey, mergeDigest.quantile(Config.getInstance().getPercentile() / 100.0));
        }
        endDur = System.currentTimeMillis();
        System.out.println("Create TDigests and calculate percentiles duration (msec) = " + (endDur - startDur));
        endMemUsed = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        System.out.println("Memory delta from start = " + (endMemUsed - startMemUsed));

        totalEndDur = System.currentTimeMillis();
        System.out.println("total duration (msec) = " + ((totalEndDur - totalStartDur)));

        System.out.println(Config.getInstance().getPercentile() + "th percentile:");
        System.out.println("[");
        for (var transPercentile : transPercentiles.entrySet()) {
            var splitKey = transPercentile.getKey().split(",");
            System.out.println("{\"transName\":\"" + splitKey[0] + "\",\"scriptId\":\"" + splitKey[1] + "\",\"percentile\":" + transPercentile.getValue() + "},");
        }
        System.out.println("]");
    }
}
