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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;

public class PreAnalyzer implements PreAnalysis, IO {

    final private Logger LOG = LoggerFactory.getLogger(PreAnalyzer.class.getName());

    private Source task;
    private List<Attribute> attributes;
    private boolean knownDataTypes = false;
    private ProfilerConfig profilerConfig;

    private static final String TEMPORAL_PATTERN_FILE = "temporal_patterns.json";
    private static final String SPATIAL_PATTERN_FILE = "spatial_patterns.json";

    private static final LinkedHashMap<String, Pattern[]> TEMPORAL_PATTERNS = loadPatternsFromFile(TEMPORAL_PATTERN_FILE);
    private static final LinkedHashMap<String, Pattern[]> SPATIAL_PATTERNS = loadPatternsFromFile(SPATIAL_PATTERN_FILE);

    private static final Pattern _DOUBLE_PATTERN = Pattern
            .compile("[\\x00-\\x20]*[+-]?(NaN|Infinity|((((\\p{Digit}+)(\\.)?((\\p{Digit}+)?)"
                    + "([eE][+-]?(\\p{Digit}+))?)|(\\.((\\p{Digit}+))([eE][+-]?(\\p{Digit}+))?)|"
                    + "(((0[xX](\\p{XDigit}+)(\\.)?)|(0[xX](\\p{XDigit}+)?(\\.)(\\p{XDigit}+)))"
                    + "[pP][+-]?(\\p{Digit}+)))[fFdD]?))[\\x00-\\x20]*");

    private static final Pattern DOUBLE_PATTERN = Pattern.compile("^(\\+|-)?\\d+([\\,]\\d+)*([\\.]\\d+)?$");
    private static final Pattern INT_PATTERN = Pattern.compile("^(\\+|-)?\\d+$");

    private final static String[] BANNED = {"", "nan"};

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
                attributeDataType = typeOfValue(dataEntry.getValue());
            }
            if (attributeDataType == null) {
                continue; // Means that data was dirty/anomaly, so skip value
            }
            attribute.setColumnType(attributeDataType);
        }
    }

    private void estimateSemanticTypes(Map<Attribute, List<String>> data) {
        // To avoid matching spatial and temporal patterns on every value, we
        // first check if the attribute is likely to be spatial or temporal
        checkSpatialOrTemporal(data);

        for (Entry<Attribute, List<String>> entry : data.entrySet()) {
            Attribute attribute = entry.getKey();
            String spatioTemporalType = attribute.getColumnSemanticType().get("type");
            if (spatioTemporalType.equals("none")) {
                continue;
            }
            String granularity = determineGranularity(spatioTemporalType, entry.getValue());
            attribute.getColumnSemanticType().put("granularity", granularity);
        }
    }


    private void checkSpatialOrTemporal(Map<Attribute, List<String>> data) {
        for (Entry<Attribute, List<String>> entry : data.entrySet()) {
            Attribute attribute = entry.getKey();
            for (String value : entry.getValue()) {
                if (value == null) {
                    continue;
                }

                if (checkTemporalGranularity(value) != null) {
                    attribute.getColumnSemanticType().put("type", "temporal");
                    break;
                }
                if (checkSpatialGranularity(value) != null) {
                    attribute.getColumnSemanticType().put("type", "spatial");
                    break;
                }
            }
            if (!attribute.getColumnSemanticType().containsKey("type")) {
                attribute.getColumnSemanticType().put("type", "none");
            }
        }
    }

    private String determineGranularity(String spatioTemporalType, List<String> values) {
        Map<String, Integer> granularityMatchCounts = new HashMap<>();

        for (String value : values) {
            String granularity;
            if (spatioTemporalType.equals("temporal")) {
                granularity = checkTemporalGranularity(value);
            } else {
                granularity = checkSpatialGranularity(value);
            }
            if (granularity != null) {
                granularityMatchCounts.put(granularity, granularityMatchCounts.getOrDefault(granularity, 0) + 1);
            }
        }

        return granularityMatchCounts.entrySet().stream()
                .max(Entry.comparingByValue()).map(Entry::getKey).orElse("unknown");
    }

    private String checkTemporalGranularity(String value) {
        for (Map.Entry<String, Pattern[]> entry : TEMPORAL_PATTERNS.entrySet()) {
            for (Pattern pattern : entry.getValue()) {
                if (pattern.matcher(value).matches()) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }

    private String checkSpatialGranularity(String value) {
        for (Map.Entry<String, Pattern[]> entry : SPATIAL_PATTERNS.entrySet()) {
            for (Pattern pattern : entry.getValue()) {
                if (pattern.matcher(value).matches()) {
                    return entry.getKey();
                }
            }
        }
        return null;
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

    public static AttributeType typeOfValue(List<String> values) {
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
