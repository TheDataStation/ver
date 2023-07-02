/**
 * @author Raul - raulcf@csail.mit.edu
 */
package ddprofiler.analysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ddprofiler.analysis.config.AnalyzerConfig;
import ddprofiler.analysis.modules.Cardinality;
import ddprofiler.analysis.modules.CardinalityAnalyzer;
import ddprofiler.analysis.modules.EntityAnalyzer;
import ddprofiler.analysis.modules.KMinHash;
import ddprofiler.analysis.modules.LabelAnalyzer;
import ddprofiler.analysis.modules.XSystemAnalyzer;
import ddprofiler.core.config.ProfilerConfig;
import xsystem.layers.XStructure;

public class TextualAnalyzer implements TextualAnalysis {

    private List<DataConsumer> analyzers;
    private CardinalityAnalyzer ca;
    private KMinHash mh;
    private XSystemAnalyzer xa;
    private EntityAnalyzer ea;
    private LabelAnalyzer la;

    private TextualAnalyzer(ProfilerConfig pc, int pseudoRandomSeed) {
        analyzers = new ArrayList<>();

        if (AnalyzerConfig.getCardinality()) {
            ca = new CardinalityAnalyzer();
            analyzers.add(ca);
        }

        if (AnalyzerConfig.getKminhash()) {
            mh = new KMinHash(pseudoRandomSeed);
            analyzers.add(mh);
        }

//        if (AnalyzerConfig.getEntity()) {
//            ea = new EntityAnalyzer();
//            analyzers.add(ea);
//        }

        if (AnalyzerConfig.getXsystem()) {
            xa = new XSystemAnalyzer();
            analyzers.add(xa);
        }

        if (AnalyzerConfig.getLabel()) {
            la = new LabelAnalyzer(pc);
            analyzers.add(la);
        }
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
        return (ca != null) ? ca.getCardinality() : null;
    }

//    @Override
//    public Entities getEntities() {
//        return null;
//    }

    @Override
    public long[] getMH() {
        return (mh != null) ? mh.getMH() : null;
    }

    @Override
    public XStructure getXstructure() {
        return (xa != null) ? xa.getXstructure() : null;
    }

    @Override
    public String getLabel() {
        return (la != null) ? la.getLabel() : null;
    }
}
