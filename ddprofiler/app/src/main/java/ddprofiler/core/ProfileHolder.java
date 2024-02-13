package ddprofiler.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ddprofiler.analysis.Analysis;
import ddprofiler.analysis.NumericalAnalysis;
import ddprofiler.analysis.TextualAnalysis;
import ddprofiler.analysis.modules.Cardinality;
import ddprofiler.analysis.modules.Range;
import ddprofiler.sources.deprecated.Attribute;
import ddprofiler.sources.deprecated.Attribute.AttributeType;

public class ProfileHolder {

    private List<Profile> results;

    /**
     * Used mostly for testing/profiling
     *
     * @return
     */
    public static List<Profile> makeFakeOne() {
        List<Profile> profileResults = new ArrayList<>();
        Profile wtr = new Profile(
                -1, "none", "none", "none", "none",
                "N", "none", 100, 100, 100, "entities",
                new long[2], "", 50, 50, 50, 0, 0);
        profileResults.add(wtr);
        return profileResults;
    }

    public ProfileHolder(List<Profile> results) {
        this.results = results;
    }

    public ProfileHolder(String dbName, String path, String sourceName, List<Attribute> attributes, Map<String, Analysis> analyzers) {
        List<Profile> profileResults = new ArrayList<>();
        for (Attribute attribute : attributes) {
            AttributeType attributeType = attribute.getColumnType();
            Analysis analysis = analyzers.get(attribute.getColumnName());
            long id = Utils.computeAttrId(dbName, sourceName, attribute.getColumnName());
            String dataType = null;
            String columnLabel = null;
            int totalValues = 0;
            int uniqueValues = 0;
            int nonEmptyValues = 0;
            String entities = null;
            long[] minhash = null;
            String xstructure = null;
            float minValue = 0;
            float maxValue = 0;
            float avgValue = 0;
            long median = 0;
            long iqr = 0;
            String spatialTemporalType = null;
            String granularity = null;

            if (attributeType.equals(AttributeType.FLOAT)) {
                NumericalAnalysis numericalAnalysis = ((NumericalAnalysis) analysis);
                Cardinality cardinality = numericalAnalysis.getCardinality();
                Range numericalRange = numericalAnalysis.getNumericalRange(AttributeType.FLOAT);

                dataType = "N";
                totalValues = (cardinality != null) ? (int) cardinality.getTotalRecords() : 0;
                uniqueValues = (cardinality != null) ? (int) cardinality.getUniqueElements() : 0;
                nonEmptyValues = (cardinality != null) ? (int) cardinality.getNonEmptyValues() : 0;
                minValue = (numericalRange != null) ? numericalRange.getMinF() : 0;
                maxValue = (numericalRange != null) ? numericalRange.getMaxF() : 0;
                avgValue = (numericalRange != null) ? numericalRange.getAvg() : 0;
                median = (numericalRange != null) ? numericalRange.getMedian() : 0;
                iqr = (numericalRange != null) ? numericalRange.getIQR() : 0;
            } else if (attributeType.equals(AttributeType.INT)) {
                NumericalAnalysis numericalAnalysis = ((NumericalAnalysis) analysis);
                Cardinality cardinality = numericalAnalysis.getCardinality();
                Range numericalRange = numericalAnalysis.getNumericalRange(AttributeType.INT);

                dataType = "N";
                totalValues = (cardinality != null) ? (int) cardinality.getTotalRecords() : 0;
                uniqueValues = (cardinality != null) ? (int) cardinality.getUniqueElements() : 0;
                nonEmptyValues = (cardinality != null) ? (int) cardinality.getNonEmptyValues() : 0;
                minValue = (numericalRange != null) ? numericalRange.getMin() : 0;
                maxValue = (numericalRange != null) ? numericalRange.getMax() : 0;
                avgValue = (numericalRange != null) ? numericalRange.getAvg() : 0;
                median = (numericalRange != null) ? numericalRange.getMedian() : 0;
                iqr = (numericalRange != null) ? numericalRange.getIQR() : 0;
            } else if (attributeType.equals(AttributeType.STRING)) {
                TextualAnalysis textualAnalysis = ((TextualAnalysis) analysis);
                Cardinality cardinality = textualAnalysis.getCardinality();

                dataType = "T";
                columnLabel = textualAnalysis.getLabel();
                totalValues = (cardinality != null) ? (int) cardinality.getTotalRecords() : 0;
                uniqueValues = (cardinality != null) ? (int) cardinality.getUniqueElements() : 0;
                nonEmptyValues = (cardinality != null) ? (int) cardinality.getNonEmptyValues() : 0;
                entities = "entities_removed_on_modernize_ddprofiler";
                minhash = (textualAnalysis.getMH() != null) ? textualAnalysis.getMH() : null;
                xstructure = (textualAnalysis.getXstructure() != null) ? textualAnalysis.getXstructure().toString() : null;
            }

            Profile columnProfile = new Profile(
                    id,
                    dbName,
                    path,
                    sourceName,
                    attribute.getColumnName(),
                    dataType,
                    columnLabel,
                    totalValues,
                    uniqueValues,
                    nonEmptyValues,
                    entities,
                    minhash,
                    xstructure,
                    minValue,
                    maxValue,
                    avgValue,
                    median,
                    iqr,
                    spatialTemporalType,
                    granularity
            );
            profileResults.add(columnProfile);
        }
        this.results = profileResults;
    }

    public List<Profile> get() {
        return results;
    }

}