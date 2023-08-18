package ddprofiler.analysis.modules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ddprofiler.analysis.TextualDataConsumer;
import ddprofiler.core.config.ProfilerConfig;
import xsystem.layers.XStructure;
import xsystem.learning.LearningModel;
import xsystem.learning.XStructType;

public class LabelAnalyzer implements TextualDataConsumer {

    final private Logger LOG = LoggerFactory.getLogger(LabelAnalyzer.class.getName());

    private ProfilerConfig pc;
    private double scoreThreshold;
    private static ArrayList<XStructType> xStructureReference = null;
    private String label;

    public LabelAnalyzer(ProfilerConfig pc) {
        this.pc = pc;
        this.scoreThreshold = pc.getDouble(ProfilerConfig.XSYSTEM_SIMILARITY_THRESHOLD);

        if (xStructureReference == null) {
            String referenceFilePath = pc.getString(ProfilerConfig.XSYSTEM_REFERENCE_FILE);
            xStructureReference = (new LearningModel()).readXStructsfromJSON(referenceFilePath);
        }
    }

    @Override
    public boolean feedTextData(List<String> records) {
        if (records == null || records.isEmpty()) {
            LOG.warn("No records to analyze");
        }
        if (LabelAnalyzer.xStructureReference == null) {
            LOG.warn("Reference file not initialized");
        }
        label = labelListOfStrings((ArrayList<String>) records);
        return true;
    }

    private String labelListOfStrings(ArrayList<String> strings) {
        String label = null;
        ArrayList<Double> scores = new ArrayList<>();
        XStructure toBeLabeled = (new XStructure()).addNewLines(strings);

        for (XStructType struct : xStructureReference) {
            double score = struct.xStructure.compareTwo(toBeLabeled, struct.xStructure);
            scores.add(score);
        }
        double maxScore = Collections.max(scores);
        int maxIndex = scores.indexOf(maxScore);

        if (maxScore >= scoreThreshold) {
            label = xStructureReference.get(maxIndex).type;
            return label;
        }

        return label;
    }

    public static void nullifyXStructureReference() {
        LabelAnalyzer.xStructureReference = null;
    }

    public String getLabel() {
        return label;
    }
    
}
