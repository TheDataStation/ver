/**
 * @author ra-mit
 * @author Sibo Wang (edit)
 */
package ddprofiler.preanalysis;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.exceptions.CsvValidationException;
import ddprofiler.core.config.ProfilerConfig;
import ddprofiler.sources.Source;
import ddprofiler.sources.deprecated.Attribute;
import ddprofiler.sources.deprecated.Attribute.AttributeType;
import ddprofiler.sources.deprecated.Attribute.AttributeSemanticType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PreAnalyzer implements PreAnalysis, IO {

    final private Logger LOG = LoggerFactory.getLogger(PreAnalyzer.class.getName());

    private Source task;
    private List<Attribute> attributes;
    private boolean knownDataTypes = false;
    private ProfilerConfig profilerConfig;

    private static final String TEMPORAL_PATTERN_FILE = "temporal_patterns.json";
    private static final String SPATIAL_PATTERN_FILE = "spatial_patterns.json";

    private static final LinkedHashMap<String, Pattern[]> TEMPORAL_PATTERNS = loadPatternsFromFile(
            TEMPORAL_PATTERN_FILE);
    private static final LinkedHashMap<String, Pattern[]> SPATIAL_PATTERNS = loadPatternsFromFile(
            SPATIAL_PATTERN_FILE);

    private static final Pattern _DOUBLE_PATTERN = Pattern
            .compile("[\\x00-\\x20]*[+-]?(NaN|Infinity|((((\\p{Digit}+)(\\.)?((\\p{Digit}+)?)"
                    + "([eE][+-]?(\\p{Digit}+))?)|(\\.((\\p{Digit}+))([eE][+-]?(\\p{Digit}+))?)|"
                    + "(((0[xX](\\p{XDigit}+)(\\.)?)|(0[xX](\\p{XDigit}+)?(\\.)(\\p{XDigit}+)))"
                    + "[pP][+-]?(\\p{Digit}+)))[fFdD]?))[\\x00-\\x20]*");

    private static final Pattern DOUBLE_PATTERN = Pattern.compile("^(\\+|-)?\\d+([\\,]\\d+)*([\\.]\\d+)?$");
    private static final Pattern INT_PATTERN = Pattern.compile("^(\\+|-)?\\d+$");

    private final static String[] BANNED = { "", "nan" };

    private static final double PATTERN_THRESHOLD = 0.5;

    private static LinkedHashMap<String, Pattern[]> loadPatternsFromFile(String filename) {
        LinkedHashMap<String, Pattern[]> patterns = new LinkedHashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        try {
            InputStream inputStream = PreAnalyzer.class.getClassLoader().getResourceAsStream(filename);
            Map<String, Pattern[]> patternMap = mapper.readValue(inputStream, new TypeReference<>() {
            });
            patterns.putAll(patternMap);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return patterns;
    }

    public PreAnalyzer(ProfilerConfig profilerConfig) {
        this.profilerConfig = profilerConfig;
    }

    /**
     * Implementation of IO interface
     *
     * @throws Exception
     */

    @Override
    public Map<Attribute, Values> readRows(int numRows) {
        Map<Attribute, List<String>> data = null;
        try {
            data = task.readRows(numRows);
            if (data == null)
                return null;
        } catch (IOException | SQLException | CsvValidationException e) {
            e.printStackTrace();
        }

        // Calculate data types if not known yet
        if (!knownDataTypes) {
            calculateDataTypes(data);
            estimateSemanticTypes(data);
        }

        Map<Attribute, Values> castData = new HashMap<>();
        // Cast map to the type
        for (Entry<Attribute, List<String>> dataEntry : data.entrySet()) {
            Values vs = null;
            AttributeType at = dataEntry.getKey().getColumnType();
            if (at.equals(AttributeType.FLOAT)) {
                List<Float> castValues = new ArrayList<>();
                vs = Values.makeFloatValues(castValues);
                for (String s : dataEntry.getValue()) {
                    float f = 0f;
                    if (DOUBLE_PATTERN.matcher(s).matches()) {
                        s = s.replace(",", ""); // remove commas, floats should
                        // be indicated with dots
                        f = Float.valueOf(s).floatValue();
                    } else {
                        continue;
                    }
                    castValues.add(f);
                }
            } else if (at.equals(AttributeType.INT)) {
                List<Long> castValues = new ArrayList<>();
                vs = Values.makeIntegerValues(castValues);
                int successes = 0;
                int errors = 0;
                for (String s : dataEntry.getValue()) {
                    long f = 0;
                    if (INT_PATTERN.matcher(s).matches()) {
                        try {
                            f = Long.valueOf(s).longValue();
                            successes++;
                        } catch (NumberFormatException nfe) {
                            LOG.warn("Error while parsing: {}", nfe.getMessage());
                            errors++;
                            // Check the ratio error-success is still ok
                            int totalRecords = successes + errors;
                            if (totalRecords > 1000) {
                                float ratio = (float) successes / errors;
                                if (ratio > 0.3) {
                                    break;
                                }
                            }
                            continue; // skip problematic value
                        }
                    } else {
                        continue;
                    }
                    castValues.add(f);
                }
            } else if (at.equals(AttributeType.STRING)) {
                List<String> castValues = new ArrayList<>();
                vs = Values.makeStringValues(castValues);
                dataEntry.getValue().forEach(s -> castValues.add(s));
            }

            castData.put(dataEntry.getKey(), vs);
        }

        return castData;
    }

    private void calculateDataTypes(Map<Attribute, List<String>> data) {
        // estimate data type for each attribute
        for (Entry<Attribute, List<String>> dataEntry : data.entrySet()) {
            Attribute attribute = dataEntry.getKey();

            // If the type is known, skip
            if (!attribute.getColumnType().equals(AttributeType.UNKNOWN))
                continue;

            AttributeType attributeDataType;
            if (profilerConfig.getBoolean(ProfilerConfig.EXPERIMENTAL)) {
                // In experimental mode - force all data to be strings
                attributeDataType = AttributeType.STRING;
            } else {
                attributeDataType = dataTypeOfValue(dataEntry.getValue());
            }
            if (attributeDataType == null) {
                continue; // Means that data was dirty/anomaly, so skip value
            }
            attribute.setColumnType(attributeDataType);
        }
    }

    /**
     * Estimate the semantic type of each attribute of a data using regex patterns.
     * 
     * @param data a Map where each key is an attribute and each value is a List of
     *             values
     */
    private void estimateSemanticTypes(Map<Attribute, List<String>> data) {
        for (Entry<Attribute, List<String>> dataEntry : data.entrySet()) {
            Attribute attribute = dataEntry.getKey();

            /*
             * Process the attribute based on the following conditions:
             * 1. If it is null, then this is the first time it is being checked.
             * 2. Otherwise, it is not yet determined as spatial or temporal in previous readRows call.
             */
            if (attribute.getColumnSemanticType() == null
                    || attribute.getColumnSemanticType() == AttributeSemanticType.NONE) {
                List<String> validValues = dataEntry.getValue().stream()
                        .filter(value -> value != null && !value.isBlank())
                        .map(value -> value.replaceAll("[\t\n\r]", " ").strip())
                        .collect(Collectors.toList());

                // Check for the temporal patterns.
                String temporalGranularity = getGranularity(TEMPORAL_PATTERNS, validValues);
                if (!temporalGranularity.equals("")) {
                    attribute.setColumnSemanticType(AttributeSemanticType.TEMPORAL);
                    attribute.getColumnSemanticTypeDetails().put(
                            "granularity",
                            temporalGranularity);
                    continue; // No need to check the spatial patterns.
                }

                // Temporal patterns failed; check for the spatial patterns.
                String spatialGranularity = getGranularity(SPATIAL_PATTERNS, validValues);
                if (!spatialGranularity.equals("")) {
                    attribute.setColumnSemanticType(AttributeSemanticType.SPATIAL);
                    attribute.getColumnSemanticTypeDetails().put(
                            "granularity",
                            spatialGranularity);
                }

                // Temporal & spatial patterns failed; set the semanticType to NONE.
                if (attribute.getColumnSemanticType() == null) {
                    attribute.setColumnSemanticType(AttributeSemanticType.NONE);
                }
            }
        }
    }

    /**
     * Given some granularity patterns (LinkedHashMap), check whether any of them
     * matches > threshold
     * and return the granularity (proritize earlier ones). If none, return an empty
     * string.
     * 
     * @param patterns a LinkedHashMap where each key is a granularity and each
     *                 value is a regex pattern
     * @param values   a List of values to be matched, which is assumed to be clean
     *                 (no null and empty values)
     * @return the granularity of the pattern that matches > threshold (else an
     *         empty string)
     */
    private String getGranularity(LinkedHashMap<String, Pattern[]> patterns, List<String> values) {
        for (Map.Entry<String, Pattern[]> patternsEntry : patterns.entrySet()) {
            for (Pattern pattern : patternsEntry.getValue()) {
                int matchCount = 0;
                for (String value : values) {
                    if (pattern.matcher(value).find()) {
                        matchCount++;
                        if ((double) matchCount / values.size() > PATTERN_THRESHOLD) {
                            return patternsEntry.getKey();
                        }
                    }
                }
            }
        }
        return "";
    }

    private String getTemporalGranularity(String value) {
        for (Map.Entry<String, Pattern[]> patternsEntry : TEMPORAL_PATTERNS.entrySet()) {
            for (Pattern temporalPattern : patternsEntry.getValue()) {
                if (temporalPattern.matcher(value).matches()) {
                    return patternsEntry.getKey();
                }
            }
        }
        return "";
    }

    public static boolean isNumerical(String s) {
        return DOUBLE_PATTERN.matcher(s).matches();
    }

    private static boolean isInteger(String s) {
        if (s == null) {
            return false;
        }

        int length = s.length();
        if (length == 0) {
            return false;
        }
        int i = 0;
        if (s.charAt(0) == '-' || s.charAt(0) == '+') {
            if (length == 1) {
                return false;
            }
            i = 1;
        }
        for (; i < length; i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }

    /*
     * exception based type checker private static boolean
     */
    public static boolean isNumericalException(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }

    /**
     * Figure out data type
     *
     * @param values
     * @return
     */

    public static AttributeType dataTypeOfValue(List<String> values) {
        boolean isFloat = false;
        boolean isInt = false;
        int floatMatches = 0;
        int intMatches = 0;
        int strMatches = 0;
        for (String s : values) {
            s = s.trim();
            if (isBanned(s)) {
                // TODO: we'll piggyback at this point to figure out how to
                // report
                // cleanliness profile
                continue;
            }
            if (isNumerical(s)) {
                if (isInteger(s)) {
                    intMatches++;
                } else {
                    floatMatches++;
                }
            } else {
                strMatches++;
            }
        }

        if (strMatches == 0) {
            if (floatMatches > 0)
                isFloat = true;
            else if (intMatches > 0)
                isInt = true;
        }

        if (isFloat)
            return AttributeType.FLOAT;
        if (isInt)
            return AttributeType.INT;
        return AttributeType.STRING;
    }

    private static boolean isBanned(String s) {
        String toCompare = s.trim().toLowerCase();
        for (String ban : BANNED) {
            if (toCompare.equals(ban)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Implementation of PreAnalysis interface
     */

    @Override
    public void assignSourceTask(Source task) {
        this.task = task;
        try {
            this.attributes = task.getAttributes();
        } catch (IOException | SQLException | CsvValidationException e) {
            e.printStackTrace();
        }
    }

    @Override
    public DataQualityReport getQualityReport() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Attribute> getEstimatedDataTypes() {
        return attributes;
    }
}
