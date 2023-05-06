package ddprofiler.analysis;

import ddprofiler.analysis.modules.EntityAnalyzer;

public class AnalyzerFactory {

    public static NumericalAnalysis makeNumericalAnalyzer() {
        NumericalAnalyzer na = NumericalAnalyzer.makeAnalyzer();
        return na;
    }

    public static TextualAnalysis makeTextualAnalyzer(EntityAnalyzer ea, int pseudoRandomSeed, String excludedAnalyzer) {
        TextualAnalyzer ta = TextualAnalyzer.makeAnalyzer(pseudoRandomSeed, excludedAnalyzer);
        return ta;
    }

    public static TextualAnalysis makeTextualAnalyzer(int pseudoRandomSeed, String excludedAnalyzer) {
//        EntityAnalyzer ea = new EntityAnalyzer();
        TextualAnalyzer ta = TextualAnalyzer.makeAnalyzer(pseudoRandomSeed, excludedAnalyzer);
        return ta;
    }
}
