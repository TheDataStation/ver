package ddprofiler.analysis.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

public class ProfileSchema {

    @JsonProperty
    private JsonNode attributes;

    @JsonProperty
    private List<ProfileAnalyzers> analyzers;

    public JsonNode getAttributes() {
        return attributes;
    }

    public List<ProfileAnalyzers> getAnalyzers() {
        return analyzers;
    }

}
