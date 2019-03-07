package SrlTDigestBenchmarks.TransDigest;

import Common.DbConn;
import Common.LgData;
import Common.SrlConsts;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.sql.*;


public class TransAggsPerformanceBench {
    private static Random gen = new Random();
    private static long startDur;
    private static long endDur;

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

    static void connectToDB() throws SQLException {
        String url = "jdbc:postgresql://localhost:32768/postgres";
        Properties props = new Properties();
        props.setProperty("user","postgres");
        props.setProperty("password","tdigest-pg");
        //props.setProperty("ssl","true");
        DbConn.getInstance().init(url, props);
    }

    public static void main(String[] args) throws SQLException {
        if (args.length != 2) {
            System.out.println("Usage: aggregatorNum ThreadsNum");
            System.exit(-1);
        }
        int aggsNum = Integer.parseInt(args[0]);
        int threadsNum = Integer.parseInt(args[1]);
        var lgsData = new ArrayList<LgData>(SrlConsts.MaxLgs);

/*        connectToDB();

        // make sure autocommit is off
        DbConn.getInstance().getDbConnection().setAutoCommit(false);
        Statement st = DbConn.getInstance().getDbConnection().createStatement();

        // Turn use of the cursor on.
        st.setFetchSize(50);
        ResultSet rs = st.executeQuery("SELECT * FROM transaction_dimensions");
        while (rs.next()) {
            System.out.println("Trans Name = " + rs.getString(2));
        }
        rs.close();

        // Turn the cursor off.
        st.setFetchSize(0);

        // Close the statement.
        st.close();*/

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

        var resAgg = new TransResAgg(lgsData, transNames.size(), threadsNum);
        try {
            resAgg.createTDigests();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
