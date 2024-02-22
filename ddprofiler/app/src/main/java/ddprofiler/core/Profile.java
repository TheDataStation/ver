package ddprofiler.core;

public record Profile(
        long id,
        String dbName,
        String path,
        String sourceName,
        String columnName,
        String dataType,
        String columnLabel,
        int totalValues,
        int uniqueValues,
        int nonEmptyValues,
        String entities,
        long[] minhash,
        String xstructure,
        float minValue,
        float maxValue,
        float avgValue,
        long median,
        long iqr,
        String semanticType
) {
    public Profile(
            long id,
            String dbName,
            String path,
            String sourceName,
            String columnName,
            String dataType,
            String columnLabel,
            int totalValues,
            int uniqueValues,
            int nonEmptyValues,
            String entities,
            long[] minhash,
            String xstructure,
            float minValue,
            float maxValue,
            float avgValue,
            long median,
            long iqr
    ) {

        // Made for backwards compatibility of when semanticType were not part of the Profile
        this(id, dbName, path, sourceName, columnName, dataType, columnLabel, totalValues, uniqueValues, nonEmptyValues,
                entities, minhash, xstructure, minValue, maxValue, avgValue, median, iqr,
                "");
    }

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