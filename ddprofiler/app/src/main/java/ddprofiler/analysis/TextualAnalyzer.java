/**
 * @author Raul - raulcf@csail.mit.edu
 */
package ddprofiler.analysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ddprofiler.analysis.modules.Cardinality;
import ddprofiler.analysis.modules.CardinalityAnalyzer;
import ddprofiler.analysis.modules.EntityAnalyzer;
import ddprofiler.analysis.modules.KMinHash;
import ddprofiler.analysis.modules.XSystemAnalyzer;
import xsystem.layers.XStructure;

public class TextualAnalyzer implements TextualAnalysis {

    private List<DataConsumer> analyzers;
    private CardinalityAnalyzer ca;
    private KMinHash mh;
    private XSystemAnalyzer xa;
    private EntityAnalyzer ea;

    private TextualAnalyzer(int pseudoRandomSeed, String excludedAnalyzer) {
        analyzers = new ArrayList<>();
        mh = new KMinHash(pseudoRandomSeed);
        ca = new CardinalityAnalyzer();
//        this.ea = ea;
        analyzers.add(ca);
        analyzers.add(mh);
//        analyzers.add(ea);

        if (!excludedAnalyzer.contains("XSystem")) {
            xa = new XSystemAnalyzer();
            analyzers.add(xa);
        }
    }

    public static TextualAnalyzer makeAnalyzer(int pseudoRandomSeed, String excludedAnalyzer) {
//        ea2.clear();
        return new TextualAnalyzer(pseudoRandomSeed, excludedAnalyzer);
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
    public XStructure getXstructure() {
        return (xa == null) ? null : xa.getXstructure();
    }

}
