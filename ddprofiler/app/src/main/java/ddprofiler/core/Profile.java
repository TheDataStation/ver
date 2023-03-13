package ddprofiler.core;

public record Profile (long id,
                       String dbName,
                       String path,
                       String sourceName,
                       String columnName,
                       String dataType,
                       int totalValues,
                       int uniqueValues,
                       int nonEmptyValues,
                       String entities,
                       long[] minhash,
                       float minValue,
                       float maxValue,
                       float avgValue,
                       long median,
                       long iqr) {

    public float minValue() {
        if (Double.isNaN(minValue) || Double.isInfinite(minValue)) {
            return 0;
        }
        return minValue;
    }

    public float maxValue() {
        if (Double.isNaN(maxValue) || Double.isInfinite(maxValue)) {
            return 0;
        }
        return maxValue;
    }

    public float avgValue() {
        if (Double.isNaN(avgValue) || Double.isInfinite(avgValue)) {
            return 0;
        }
        return avgValue;
    }
}