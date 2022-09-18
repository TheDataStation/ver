/**
 * @author Raul - raulcf@csail.mit.edu
 */
package ddprofiler.analysis.modules;

public class Cardinality {

    private long totalRecords;
    private long uniqueElements;
    private long nonEmpty;

    public Cardinality(long totalRecords, long uniqueElements, long nonEmpty) {
        this.totalRecords = totalRecords;
        this.uniqueElements = uniqueElements;
        this.nonEmpty = nonEmpty;
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    public long getUniqueElements() {
        return uniqueElements;
    }

    public long getNonEmptyValues() {
        return nonEmpty;
    }

    @Override
    public String toString() {
        return uniqueElements + "/" + totalRecords;
    }
}
