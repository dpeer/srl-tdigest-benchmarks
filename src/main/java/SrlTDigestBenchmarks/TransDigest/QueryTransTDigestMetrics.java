package SrlTDigestBenchmarks.TransDigest;

import Common.*;
import Common.Pojos.TransDimensionPojo;
import Common.Pojos.TransTDigestMetricsPojo;
import com.tdunning.math.stats.MergingDigest;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryTransTDigestMetrics {
    public static void main(String[] args) throws SQLException {
        long totalStartDur, totalEndDur, startDur, endDur;

        boolean fromSrcDB = false;
        List<TransDimensionPojo> externalDimensions;
        Map<String, TransDimensionPojo> externalDimensionIds = new HashMap<>();
        int percentile;
        String testId = "";
        String runId = "";
        if (args.length < 1) {
            System.out.println("Usage: percentile [fromSrcDB [testId runId]]");
            System.exit(-1);
        }
        percentile = Integer.parseInt(args[0]);
        if (args.length > 1) {
            fromSrcDB = Boolean.parseBoolean(args[1]);
            if (fromSrcDB) {
                if (args.length < 4) {
                    System.out.println("Must provide testId & runId");
                    System.exit(-1);
                }
                testId = args[2];
                runId = args[3];
            }
        }

        DbConn.init();

        if (fromSrcDB) {
            if (!DbConnSrc.init()) {
                System.out.println("Failed to connect to Source DB");
                System.exit(-1);
            }

            startDur = System.currentTimeMillis();
            externalDimensions = DalSrc.getDimensions(testId, runId);
            endDur = System.currentTimeMillis();
            System.out.println("Main: Get source dimensions duration (msec) = " + ((endDur - startDur)) + "; Dimensions count = " + externalDimensions.size());

            for (var externalDimension : externalDimensions) {
                externalDimensionIds.put(externalDimension.getId(), externalDimension);
            }
        }

        totalStartDur = System.currentTimeMillis();

        System.out.println("QueryTransTDigestMetrics.main: Retrieve TDigestTransMetrics from DB");
        startDur = System.currentTimeMillis();

        var tdigestTransMetrics = Dal.getTDigestTransMetrics();

        endDur = System.currentTimeMillis();
        System.out.println("QueryTransTDigestMetrics.main: Retrieve TDigestTransMetrics from DB duration (msec) = " + (endDur - startDur));


        System.out.println("QueryTransTDigestMetrics.main: Group data per transaction");
        startDur = System.currentTimeMillis();

        var transGroups = tdigestTransMetrics.stream().collect(Collectors.groupingBy(TransTDigestMetricsPojo::getTransactionId));

        // Todo: Add multi threaded implementation.

        endDur = System.currentTimeMillis();
        System.out.println("QueryTransTDigestMetrics.main: Group data per transaction duration (msec) = " + (endDur - startDur) + "; #Transaction = " + transGroups.size());

        System.out.println("QueryTransTDigestMetrics.main: Create TDigests and calculate percentiles");
        startDur = System.currentTimeMillis();

        var transPercentiles = new HashMap<String, Double>(transGroups.size());

        for (var transGroupEntry : transGroups.entrySet()) {
            var mergeDigest = new MergingDigest(SrlConsts.TdAggCompression);
            for (var transTDigestMetricsPojo : transGroupEntry.getValue()) {
                mergeDigest.add(MergingDigest.fromBytes(ByteBuffer.wrap(transTDigestMetricsPojo.getTdigestBuf())));
            }
            String percentileKey;
            if (fromSrcDB) {
                var extDimensionId = externalDimensionIds.get(transGroupEntry.getKey());
                percentileKey = extDimensionId.getTransactionName() + " (" + extDimensionId.getScriptId() + ")";
            } else {
                percentileKey = transGroupEntry.getKey();
            }
            transPercentiles.put(percentileKey, mergeDigest.quantile(percentile / 100));
        }
        endDur = System.currentTimeMillis();
        System.out.println("QueryTransTDigestMetrics.main: Create TDigests and calculate percentiles duration (msec) = " + (endDur - startDur));

        totalEndDur = System.currentTimeMillis();
        System.out.println("Main: total duration (msec) = " + ((totalEndDur - totalStartDur)));

        for (var transPercentile : transPercentiles.entrySet()) {
            System.out.println("Main: TransactionId = " + transPercentile.getKey() + "; " + percentile + "th percentile = " + transPercentile.getValue());
        }
    }
}
