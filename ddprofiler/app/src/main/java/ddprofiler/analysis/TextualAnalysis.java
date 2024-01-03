package ddprofiler.analysis;

import ddprofiler.analysis.modules.Entities;
import xsystem.layers.XStructure;

/**
 * @author Raul - raulcf@csail.mit.edu
 */
public interface TextualAnalysis extends Analysis, TextualDataConsumer {

//    public Entities getEntities();

    public long[] getMH();

    public XStructure getXstructure();

    public String getLabel();
}
