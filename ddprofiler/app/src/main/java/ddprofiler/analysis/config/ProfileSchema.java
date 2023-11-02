package ddprofiler.analysis.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

public class ProfileSchema {

    @JsonProperty
    private JsonNode attributes;

    @JsonProperty
    private List<ProfileAnalyzer> analyzers;

    public JsonNode getAttributes() {
        return attributes;
    }

    public List<ProfileAnalyzer> getAnalyzers() {
        return analyzers;
    }
}
