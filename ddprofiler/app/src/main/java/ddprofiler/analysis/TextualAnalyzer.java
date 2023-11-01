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
import ddprofiler.analysis.modules.Entities;
import ddprofiler.analysis.modules.EntityAnalyzer;
import ddprofiler.analysis.modules.KMinHash;

public class TextualAnalyzer implements TextualAnalysis {

    private List<DataConsumer> analyzers;
    private CardinalityAnalyzer ca;
    private KMinHash mh;
    private EntityAnalyzer ea;
<<<<<<< HEAD

    private TextualAnalyzer(int pseudoRandomSeed) {
        analyzers = new ArrayList<>();
        mh = new KMinHash(pseudoRandomSeed);
        ca = new CardinalityAnalyzer();
//        this.ea = ea;
        analyzers.add(ca);
        analyzers.add(mh);
//        analyzers.add(ea);
=======
    private LabelAnalyzer la;

    private TextualAnalyzer(int pseudoRandomSeed, ProfilerConfig pc) {
        analyzers = new ArrayList<>();
        if (AnalyzerConfig.getCardinality().getEnabled()) {
            ca = new CardinalityAnalyzer();
            analyzers.add(ca);
        }

        if (AnalyzerConfig.getKminhash().getEnabled()) {
            mh = new KMinHash(pseudoRandomSeed);
            analyzers.add(mh);
        }

        if (AnalyzerConfig.getXsystem().getEnabled()) {
            xa = new XSystemAnalyzer();
            analyzers.add(xa);
        }

        if (AnalyzerConfig.getLabel().getEnabled()) {
            la = new LabelAnalyzer(pc);
            analyzers.add(la);
        }
>>>>>>> aff10a7cee35a7ef0ed44b20946e857e0c226c57
    }

    public static TextualAnalyzer makeAnalyzer(int pseudoRandomSeed) {
//        ea2.clear();
        return new TextualAnalyzer(pseudoRandomSeed);
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

    @Override
    public long[] getMH() {
        return (mh != null) ? mh.getMH() : null;
    }

<<<<<<< HEAD
=======
    @Override
    public XStructure getXstructure() {
        return (xa != null) ? xa.getXstructure() : null;
    }

    @Override
    public String getLabel() {
        return (la != null) ? la.getLabel() : null;
    }
>>>>>>> aff10a7cee35a7ef0ed44b20946e857e0c226c57
}
