package ddprofiler.analysis.config;

public class AnalyzerConfig {
    private static boolean cardinality;
    private static boolean entity;
    private static boolean kminhash;
    private static boolean range;
    private static boolean xsystem;
    private static boolean label;

    public static boolean getCardinality() {
        return cardinality;
    }

    public static boolean getEntity() {
        return entity;
    }

    public static boolean getKminhash() {
        return kminhash;
    }

    public static boolean getRange() {
        return range;
    }

    public static boolean getXsystem() {
        return xsystem;
    }

    public static boolean getLabel() {
        return label;
    }

    public static void setCardinality(boolean cardinality) {
        AnalyzerConfig.cardinality = cardinality;
    }

    public static void setEntity(boolean entity) {
        AnalyzerConfig.entity = entity;
    }

    public static void setKminhash(boolean kminhash) {
        AnalyzerConfig.kminhash = kminhash;
    }

    public static void setRange(boolean range) {
        AnalyzerConfig.range = range;
    }

    public static void setXsystem(boolean xsystem) {
        AnalyzerConfig.xsystem = xsystem;
    }

    public static void setLabel(boolean label) {
        AnalyzerConfig.label = label;
    }
}
