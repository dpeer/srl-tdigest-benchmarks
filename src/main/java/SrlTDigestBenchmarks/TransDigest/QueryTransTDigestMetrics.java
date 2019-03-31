package SrlTDigestBenchmarks.TransDigest;

import Common.*;
import Common.Pojos.TransDimensionPojo;
import Common.Pojos.TransTDigestMetricsPojo;
import com.google.gson.Gson;
import com.tdunning.math.stats.MergingDigest;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryTransTDigestMetrics {
    public static void main(String[] args) throws SQLException {
        long totalStartDur, startDur, startMemUsed;

        List<TransDimensionPojo> externalDimensions;
        Map<String, TransDimensionPojo> externalDimensionIds = new HashMap<>();
        var out = new QureyTransTDigestMetricsOutput();

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
            out.getDimensionsTime = System.currentTimeMillis() - startDur;
            System.out.println("Get source dimensions duration (msec) = " + out.getDimensionsTime + "; Dimensions count = " + externalDimensions.size());

            for (var externalDimension : externalDimensions) {
                externalDimensionIds.put(externalDimension.getId(), externalDimension);
            }
        }

        startMemUsed = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        out.startMemUsed = startMemUsed;

        System.out.println("Retrieve TDigestTransMetrics from DB");
        startDur = System.currentTimeMillis();

        var tdigestTransMetrics = Dal.getTDigestTransMetrics();

        out.retrieveTransFromDBTime = System.currentTimeMillis() - startDur;
        out.retrieveTransFromDBMemDelta = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() - startMemUsed;
        System.out.println("Retrieve TDigestTransMetrics from DB duration (msec) = " + out.retrieveTransFromDBTime);
        System.out.println("Memory delta from start = " + out.retrieveTransFromDBMemDelta);


        System.out.println("Group data per transaction");
        startDur = System.currentTimeMillis();

        var transGroups = tdigestTransMetrics.stream().collect(Collectors.groupingBy(TransTDigestMetricsPojo::getTransactionId));

        // Todo: Add multi threaded implementation.

        out.groupDataPerTransTime = System.currentTimeMillis() - startDur;
        out.groupDataPerTransMemDelta = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() - startMemUsed;
        System.out.println("Group data per transaction duration (msec) = " + out.groupDataPerTransTime + "; #Transactions = " + transGroups.size());
        System.out.println("Memory delta from start = " + out.groupDataPerTransMemDelta);

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
        out.calcTDigestsTime = System.currentTimeMillis() - startDur;
        out.calcTDigestsMemDelta = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() - startMemUsed;
        System.out.println("Create TDigests and calculate percentiles duration (msec) = " + out.calcTDigestsTime);
        System.out.println("Memory delta from start = " + out.calcTDigestsMemDelta);

        out.totalTime = System.currentTimeMillis() - totalStartDur;
        System.out.println("total duration (msec) = " + out.totalTime);

        for (var transPercentile : transPercentiles.entrySet()) {
            var splitKey = transPercentile.getKey().split(",");
            out.transData.add(new TransDataPojo(splitKey[0], splitKey[1], transPercentile.getValue()));
        }

        if (args.length == 1) {
            var gson = new Gson();
            var json = gson.toJson(out);
            var outFileName = "queryTransTDigestMetrics" + "_" + Config.getInstance().getTdCompression() + "_" + Config.getInstance().getTdAggCompression() + ".json";
            try (PrintWriter outFile = new PrintWriter(args[0] + "/" + outFileName)) {
                outFile.println(json);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}

class TransDataPojo {
    String transName;
    String scriptId;
    double percentile;

    public TransDataPojo(String transName, String scriptId, double percentile) {
        this.transName = transName;
        this.scriptId = scriptId;
        this.percentile = percentile;
    }
}

class QureyTransTDigestMetricsOutput {
    Config config = Config.getInstance();
    long startMemUsed;
    long getDimensionsTime;
    long retrieveTransFromDBTime;
    long retrieveTransFromDBMemDelta;
    long groupDataPerTransTime;
    long groupDataPerTransMemDelta;
    long calcTDigestsTime;
    long calcTDigestsMemDelta;
    long totalTime;
    ArrayList<TransDataPojo> transData = new ArrayList<>();
}