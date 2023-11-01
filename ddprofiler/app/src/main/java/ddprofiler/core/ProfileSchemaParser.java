package ddprofiler.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import ddprofiler.analysis.config.AnalyzerConfig;
import ddprofiler.analysis.config.ProfileAnalyzer;
import ddprofiler.analysis.config.ProfileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public class ProfileSchemaParser {
    final private static Logger LOG = LoggerFactory.getLogger(ProfileSchemaParser.class.getName());

    public static void processProfileSchema(String path) throws Exception {
        File file = new File(path);
        ProfileSchema schema;
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            schema = mapper.readValue(file, ProfileSchema.class);
        } catch (Exception e) {
            LOG.error("While parsing {} file to read sources", file.toPath());
            throw e;
        }

        List<ProfileAnalyzer> analyzers = schema.getAnalyzers();
        for (ProfileAnalyzer analyzer : analyzers) {
            String name = analyzer.getName();

            switch (name) {
                case "cardinality" -> AnalyzerConfig.setCardinality(analyzer);
                case "entity" -> AnalyzerConfig.setEntity(analyzer);
                case "kminhash" -> AnalyzerConfig.setKminhash(analyzer);
                case "range" -> AnalyzerConfig.setRange(analyzer);
                case "xsystem" -> AnalyzerConfig.setXsystem(analyzer);
                case "label" -> AnalyzerConfig.setLabel(analyzer);
            }
        }
    }

    public static void main(String args[]) {

        // FIXME: make this a test instead
        File file = new File("./../profile_schema.yml");
        ProfileSchema schema = null;
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            schema = mapper.readValue(file, ProfileSchema.class);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println(schema.getAnalyzers().get(0).getName());
    }
}
