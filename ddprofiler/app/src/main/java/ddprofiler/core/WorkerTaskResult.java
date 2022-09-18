package ddprofiler.core;

public class WorkerTaskResult {


    final private long id;
    final private String dbName;
    final private String path;
    final private String sourceName;
    final private String columnName;
    final private String dataType;
    final private int totalValues;
    final private int uniqueValues;
    final private int nonEmptyValues;
    final private String entities;
    final private long[] minhash;
    final private float minValue;
    final private float maxValue;
    final private float avgValue;
    final private long median;
    final private long iqr;

    public WorkerTaskResult(
            long id,
            String dbName,
            String path,
            String sourceName,
            String columnName,
            String dataType,
            int totalValues,
            int uniqueValues,
            int nonEmptyValues,
            String entities,
            long[] minhash) {
        this.id = id;
        this.dbName = dbName;
        this.path = path;
        this.sourceName = sourceName;
        this.columnName = columnName;
        this.dataType = dataType;
        this.totalValues = totalValues;
        this.uniqueValues = uniqueValues;
        this.nonEmptyValues = nonEmptyValues;
        this.entities = entities;
        this.minhash = minhash;
        this.minValue = 0; // non existent
        this.maxValue = 0; // non existent
        this.avgValue = 0; // non existent
        this.median = 0;   // non existent
        this.iqr = 0;       // non existent
    }

    public WorkerTaskResult(
            long id,
            String dbName,
            String path,
            String sourceName,
            String columnName,
            String dataType,
            int totalValues,
            int uniqueValues,
            int nonEmptyValues,
            float minValue,
            float maxValue,
            float avgValue,
            long median,
            long iqr) {
        this.id = id;
        this.dbName = dbName;
        this.path = path;
        this.sourceName = sourceName;
        this.columnName = columnName;
        this.dataType = dataType;
        this.totalValues = totalValues;
        this.uniqueValues = uniqueValues;
        this.nonEmptyValues = nonEmptyValues;
        this.entities = ""; // non existent
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.avgValue = avgValue;
        this.median = median;
        this.iqr = iqr;
        this.minhash = null;
    }

    public long getId() {
        return id;
    }

    public String getDBName() {
        return dbName;
    }

    public String getPath() {
        return path;
    }

    public String getSourceName() {
        return sourceName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public int getTotalValues() {
        return totalValues;
    }

    public int getUniqueValues() {
        return uniqueValues;
    }

    public int getNonEmptyValues() {
        return nonEmptyValues;
    }

    public String getEntities() {
        return entities;
    }

    public long[] getMH() {
        return minhash;
    }

    public float getMinValue() {
        if (Double.isNaN(minValue) || Double.isInfinite(minValue)) {
            return 0;
        }
        return minValue;
    }

    public float getMaxValue() {
        if (Double.isNaN(maxValue) || Double.isInfinite(maxValue)) {
            return 0;
        }
        return maxValue;
    }

    public float getAvgValue() {
        if (Double.isNaN(avgValue) || Double.isInfinite(avgValue)) {
            return 0;
        }
        return avgValue;
    }

    public long getMedian() {
        return median;
    }

    public long getIQR() {
        return iqr;
    }

}
