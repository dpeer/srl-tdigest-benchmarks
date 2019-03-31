package Common;

import java.util.Properties;

public class Config {
    private static Config ourInstance = new Config();
    private static Properties configFile;

    private final int aggsNum;
    private final int threadsNum;
    private final int aggDurationMilliSeconds;
    private final int tdCompression;
    private final int tdAggCompression;
    private final int tdQueryCompression;
    private final int percentile;
    private final int maxLgs;
    private final int maxEmulations;
    private final int maxTransactions;
    private final int maxRegions;
    private final int lgValuesPerDimension;
    private final int numIterations;
    private final boolean saveToDB;
    private final boolean fromSrcDB;
    private final String dbHost;
    private final String dbName;
    private final String dbUser;
    private final String dbPass;
    private final String dbHostSrc;
    private final String dbNameSrc;
    private final String dbUserSrc;
    private final String dbPassSrc;
    private final String testId;
    private final String runId;

    private final int maxDimensions;
    private final int maxDimensionsPerLg;
    private final int maxTransactionsPerLgPerInterval;

    public static Config getInstance() {
        return ourInstance;
    }

    private Config() {
        configFile = new java.util.Properties();
        try {
            configFile.load(this.getClass().getClassLoader().getResourceAsStream("config.properties"));
        } catch(Exception eta){
            eta.printStackTrace();
        }

        aggsNum = getPropertyInt("aggsNum");
        threadsNum = getPropertyInt("threadsNum");
        aggDurationMilliSeconds = getPropertyInt("aggDurationMilliSeconds");
        tdCompression = getPropertyInt("tdCompression");
        tdAggCompression = getPropertyInt("tdAggCompression");
        tdQueryCompression = getPropertyInt("tdQueryCompression");
        percentile = getPropertyInt("percentile");
        maxLgs = getPropertyInt("maxLgs");
        maxEmulations = getPropertyInt("maxEmulations");
        maxTransactions = getPropertyInt("maxTransactions");
        maxRegions = getPropertyInt("maxRegions");
        lgValuesPerDimension = getPropertyInt("lgValuesPerDimension");
        numIterations = getPropertyInt("numIterations");
        saveToDB = getPropertyBool("saveToDB");
        fromSrcDB = getPropertyBool("fromSrcDB");
        dbHost = getProperty("dbHost");
        dbName = getProperty("dbName");
        dbUser = getProperty("dbUser");
        dbPass = getProperty("dbPass");
        dbHostSrc = getProperty("dbHostSrc");
        dbNameSrc = getProperty("dbNameSrc");
        dbUserSrc = getProperty("dbUserSrc");
        dbPassSrc = getProperty("dbPassSrc");
        testId = getProperty("testId");
        runId = getProperty("runId");

        maxDimensions = maxRegions * maxTransactions * maxEmulations;
        maxTransactionsPerLgPerInterval = maxTransactions / 2;
        maxDimensionsPerLg = maxTransactionsPerLgPerInterval * maxEmulations;
    }

    public String getProperty(String key) {
        String value = this.configFile.getProperty(key);
        return value;
    }

    public int getPropertyInt(String key) {
        String value = this.configFile.getProperty(key);
        return Integer.valueOf(value);
    }

    public boolean getPropertyBool(String key) {
        String value = this.configFile.getProperty(key);
        return Boolean.valueOf(value);
    }

    public int getAggsNum() {
        return aggsNum;
    }

    public int getThreadsNum() {
        return threadsNum;
    }

    public int getAggDurationMilliSeconds() {
        return aggDurationMilliSeconds;
    }

    public int getTdCompression() {
        return tdCompression;
    }

    public int getTdAggCompression() {
        return tdAggCompression;
    }

    public int getTdQueryCompression() {
        return tdQueryCompression;
    }

    public int getPercentile() {
        return percentile;
    }

    public int getMaxLgs() {
        return maxLgs;
    }

    public int getMaxEmulations() {
        return maxEmulations;
    }

    public int getMaxTransactions() {
        return maxTransactions;
    }

    public int getMaxRegions() {
        return maxRegions;
    }

    public int getLgValuesPerDimension() {
        return lgValuesPerDimension;
    }

    public int getNumIterations() {
        return numIterations;
    }

    public boolean isSaveToDB() {
        return saveToDB;
    }

    public boolean isFromSrcDB() {
        return fromSrcDB;
    }

    public String getTestId() {
        return testId;
    }

    public String getRunId() {
        return runId;
    }

    public String getDbHost() {
        return dbHost;
    }

    public String getDbName() {
        return dbName;
    }

    public String getDbUser() {
        return dbUser;
    }

    public String getDbPass() {
        return dbPass;
    }

    public String getDbHostSrc() {
        return dbHostSrc;
    }

    public String getDbNameSrc() {
        return dbNameSrc;
    }

    public String getDbUserSrc() {
        return dbUserSrc;
    }

    public String getDbPassSrc() {
        return dbPassSrc;
    }

    public int getMaxDimensions() {
        return maxDimensions;
    }

    public int getMaxDimensionsPerLg() {
        return maxDimensionsPerLg;
    }

    public int getMaxTransactionsPerLgPerInterval() {
        return maxTransactionsPerLgPerInterval;
    }
}
