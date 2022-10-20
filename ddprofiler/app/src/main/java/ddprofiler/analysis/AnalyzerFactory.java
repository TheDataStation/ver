package ddprofiler.analysis;

import ddprofiler.analysis.modules.EntityAnalyzer;

public class AnalyzerFactory {

    public static NumericalAnalysis makeNumericalAnalyzer() {
        NumericalAnalyzer na = NumericalAnalyzer.makeAnalyzer();
        return na;
    }

    public static TextualAnalysis makeTextualAnalyzer(EntityAnalyzer ea, int pseudoRandomSeed) {
        TextualAnalyzer ta = TextualAnalyzer.makeAnalyzer(pseudoRandomSeed);
        return ta;
    }

    public static TextualAnalysis makeTextualAnalyzer(int pseudoRandomSeed) {
//        EntityAnalyzer ea = new EntityAnalyzer();
        TextualAnalyzer ta = TextualAnalyzer.makeAnalyzer(pseudoRandomSeed);
        return ta;
    }
}
