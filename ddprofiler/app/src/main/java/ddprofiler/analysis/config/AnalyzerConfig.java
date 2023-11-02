package ddprofiler.analysis.config;

public class AnalyzerConfig {
    private static ProfileAnalyzer cardinality;
    private static ProfileAnalyzer entity;
    private static ProfileAnalyzer kminhash;
    private static ProfileAnalyzer range;
    private static ProfileAnalyzer xsystem;
    private static ProfileAnalyzer label;

    public static ProfileAnalyzer getCardinality() {
        return cardinality;
    }

    public static ProfileAnalyzer getEntity() {
        return entity;
    }

    public static ProfileAnalyzer getKminhash() {
        return kminhash;
    }

    public static ProfileAnalyzer getRange() {
        return range;
    }

    public static ProfileAnalyzer getXsystem() {
        return xsystem;
    }

    public static ProfileAnalyzer getLabel() {
        return label;
    }

    public static void setCardinality(ProfileAnalyzer cardinality) {
        AnalyzerConfig.cardinality = cardinality;
    }

    public static void setEntity(ProfileAnalyzer entity) {
        AnalyzerConfig.entity = entity;
    }

    public static void setKminhash(ProfileAnalyzer kminhash) {
        AnalyzerConfig.kminhash = kminhash;
    }

    public static void setRange(ProfileAnalyzer range) {
        AnalyzerConfig.range = range;
    }

    public static void setXsystem(ProfileAnalyzer xsystem) {
        AnalyzerConfig.xsystem = xsystem;
    }

    public static void setLabel(ProfileAnalyzer label) {
        AnalyzerConfig.label = label;
    }
}
