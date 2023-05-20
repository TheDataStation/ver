/**
 * @author Raul - raulcf@csail.mit.edu
 */
package ddprofiler.analysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ddprofiler.analysis.modules.Cardinality;
import ddprofiler.analysis.modules.CardinalityAnalyzer;
import ddprofiler.analysis.modules.Entities;
import ddprofiler.analysis.modules.EntityAnalyzer;
import ddprofiler.analysis.modules.KMinHash;
import ddprofiler.analysis.modules.LabelAnalyzer;
import ddprofiler.core.config.ProfilerConfig;

public class TextualAnalyzer implements TextualAnalysis {

    private List<DataConsumer> analyzers;
    private CardinalityAnalyzer ca;
    private KMinHash mh;
    private EntityAnalyzer ea;
    private LabelAnalyzer la;

    private TextualAnalyzer(int pseudoRandomSeed) {
        analyzers = new ArrayList<>();
        mh = new KMinHash(pseudoRandomSeed);
        ca = new CardinalityAnalyzer();
//        this.ea = ea;
        analyzers.add(ca);
        analyzers.add(mh);
//        analyzers.add(ea);
    }

    private TextualAnalyzer(ProfilerConfig pc, int pseudoRandomSeed) {
        analyzers = new ArrayList<>();
        la = new LabelAnalyzer(pc);
        mh = new KMinHash(pseudoRandomSeed);
        ca = new CardinalityAnalyzer();
        analyzers.add(la);
        analyzers.add(ca);
        analyzers.add(mh);
    }

    public static TextualAnalyzer makeAnalyzer(int pseudoRandomSeed) {
//        ea2.clear();
        return new TextualAnalyzer(pseudoRandomSeed);
    }

    public static TextualAnalyzer makeAnalyzer(ProfilerConfig pc, int pseudoRandomSeed) {
        return new TextualAnalyzer(pc, pseudoRandomSeed);
    }

    @Override
    public boolean feedTextData(List<String> records) {
        Iterator<DataConsumer> dcs = analyzers.iterator();
        while (dcs.hasNext()) {
            TextualDataConsumer dc = (TextualDataConsumer) dcs.next();
            dc.feedTextData(records);
        }

        return false;
    }

    @Override
    public DataProfile getProfile() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Cardinality getCardinality() {
        return ca.getCardinality();
    }

//    @Override
//    public Entities getEntities() {
//        return null;
//    }

    @Override
    public long[] getMH() {
        return mh.getMH();
    }

    @Override
    public String getLabel() {
        return la.getLabel();
    }

}
