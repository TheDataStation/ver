package ddprofiler.analysis.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

public class ProfileAnalyzer {

    @JsonProperty
    private String name;

    @JsonProperty
    private boolean enabled;

    @JsonProperty
    private JsonNode fields;

    public String getName() {
        return name;
    }

    public boolean getEnabled() {
        return enabled;
    }

    public JsonNode getFields() {
        return fields;
    }
}
