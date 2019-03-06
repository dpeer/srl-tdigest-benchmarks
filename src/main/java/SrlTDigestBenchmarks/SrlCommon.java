package SrlTDigestBenchmarks;

public class SrlCommon {
    public static double[] generatePercentileMarkers(double percentile, double perDelta, int perSteps) {
        double[] markers = new double[perSteps];

        for (int i = 0, j = (perSteps - 1) / 2 * -1; i < perSteps; i++, j++) {
            markers[i] = percentile + j * perDelta;
            if (markers[i] <= 0 || markers[i] >= 100) {
                System.out.println("percentiles out of bounds");
                System.exit(-1);
            }
        }

        return markers;
    }

    public static String createDimensionId(int region, int emulationId, int trxId) {
        return region + "-" + emulationId + "-" + trxId;
    }
}
