package ddprofiler.sources.config;

import ddprofiler.sources.SourceType;

public interface SourceConfig {

    public SourceType getSourceType();

    public String getSourceName();

    public void setSourceName(String sourceName);

    @Deprecated
    public String getRelationName();

    public String getPath();

    @Deprecated
    public SourceConfig selfCopy();

}
