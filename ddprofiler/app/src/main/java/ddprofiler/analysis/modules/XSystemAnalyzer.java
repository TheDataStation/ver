package ddprofiler.analysis.modules;

import ddprofiler.analysis.TextualDataConsumer;
import xsystem.XSystemImplementation;
import xsystem.layers.XStructure;

import java.util.ArrayList;
import java.util.List;

public class XSystemAnalyzer implements TextualDataConsumer {

    private static XSystemImplementation xsystem = new XSystemImplementation();
    private XStructure xstructure;

    @Override
    public boolean feedTextData(List<String> records) {
        ArrayList<String> recordsFiltered = new ArrayList<>();

        for (String r : records) {
            if (r != null && !r.isEmpty() && !r.trim().isEmpty()) {
                recordsFiltered.add(r);
            }
        }

        if (recordsFiltered.size() > 0) {
            xstructure = xsystem.build(recordsFiltered);
        }

        return true;
    }

    public XStructure getXstructure() {
        return xstructure;
    }
}
