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
        this.scoreThreshold = pc.getDouble("xsystem.threshold");

        if (LabelAnalyzer.xStructureReference == null && pc.getBoolean("xsystem")) {
            String referenceFilePath = pc.getString("xsystem.reference");
            LabelAnalyzer.xStructureReference = (new LearningModel()).readXStructsfromJSON(referenceFilePath);
        } else {
            LOG.warn("Reference file already initialized or XSystem is not enabled");
        }
    }

    /**
     * For testing
     * @param string
     */
    private LabelAnalyzer(String string) {
        String referenceFilePath = string;
        LabelAnalyzer.xStructureReference = (new LearningModel()).readXStructsfromJSON(referenceFilePath);
        scoreThreshold = 0.5;
    }

    @Override
    public boolean feedTextData(List<String> records) {
        if (records == null || records.isEmpty()) {
            LOG.warn("No records to analyze");
        }
        if (LabelAnalyzer.xStructureReference == null) {
            LOG.warn("Reference file not initialized");
        }
        if (pc.getBoolean("xsystem")) {
            this.label = labelListOfStrings((ArrayList<String>) records);
        } else {
            LOG.info("XSystem is not enabled");
        }
        return true;
    }

    private String labelListOfStrings(ArrayList<String> strings) {
        String label = null;
        ArrayList<Double> scoreList = new ArrayList<>();
        XStructure toBeLabeled = (new XStructure()).addNewLines(strings);

        for (XStructType struct : xStructureReference) {
            double score = struct.xStructure.compareTwo(toBeLabeled, struct.xStructure);
            scoreList.add(score);
        }
        double maxScore = Collections.max(scoreList);
        int maxIndex = scoreList.indexOf(maxScore);

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

    public static void main(String[] args) {

        LabelAnalyzer autolabel = new LabelAnalyzer(
                "D:\\Project\\ver\\ddprofiler\\app\\src\\main\\resources\\reference.json");
        ArrayList<String> test1 = new ArrayList<>();

        test1.add("12/10/2002");
        test1.add("02/11/2002");
        test1.add("22/11/2012");
        test1.add("18/01/2002");
        test1.add("07/12/2002");
        test1.add("11/12/2012");

        System.out.println(autolabel.labelListOfStrings(test1));
        ArrayList<String> test2 = new ArrayList<>();

        test2.add("12-10-2002");
        test2.add("02-11-2002");
        test2.add("22-11-2012");
        test2.add("18-01-2002");
        test2.add("07-12-2002");
        test2.add("11-12-2012");

        System.out.println(autolabel.labelListOfStrings(test2));
        ArrayList<String> test3 = new ArrayList<>();

        test3.add("(12.313414414, -31.314141414)");
        test2.add("(12.313414414, 31.314141685)");
        test2.add("(-12.313414414, 31.314141414)");
        test2.add("(112.313414414, -31.314141414)");
        test2.add("(-110.313414414, 31.3120876614)");
        test2.add("(-12.313414414, -31.3146741414)");

        System.out.println(autolabel.labelListOfStrings(test3));

    }
    
}
