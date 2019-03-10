package SrlTDigestBenchmarks.TransDigest;

import Common.Dal;
import Common.DbConn;
import Common.SrlConsts;
import Common.TransTDigestMetricsPojo;
import com.tdunning.math.stats.MergingDigest;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.stream.Collectors;

class TransCollectTDigestThread extends Thread {

}

public class QueryTransTDigestMetrics {
    public static void main(String[] args) throws SQLException {
        long totalStartDur, totalEndDur, startDur, endDur;

        DbConn.init();

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

        var transTDigests = new HashMap<String, MergingDigest>(transGroups.size());
        var transPercentiles = new HashMap<String, Double>(transGroups.size());

        for (var transGroupKey : transGroups.keySet()) {
            transTDigests.put(transGroupKey, new MergingDigest(SrlConsts.TdAggCompression));
        }

        for (var transGroupEntry : transGroups.entrySet()) {
            var mergeDigest = transTDigests.put(transGroupEntry.getKey(), new MergingDigest(SrlConsts.TdAggCompression));
            for (var transTDigestMetricsPojo : transGroupEntry.getValue()) {
                mergeDigest.add(MergingDigest.fromBytes(ByteBuffer.wrap(transTDigestMetricsPojo.getTdigestBuf())));
            }
            transPercentiles.put(transGroupEntry.getKey(), mergeDigest.quantile(0.95));
        }
        endDur = System.currentTimeMillis();
        System.out.println("QueryTransTDigestMetrics.main: Create TDigests and calculate percentiles duration (msec) = " + (endDur - startDur));

        totalEndDur = System.currentTimeMillis();
        System.out.println("Main: total duration (msec) = " + ((totalEndDur - totalStartDur)));

        for (var transPercentile : transPercentiles.entrySet()) {
            System.out.println("Main: TransactionId = " + transPercentile.getKey() + "; 95% percentile = " + transPercentile.getValue());
        }
    }
}
