/**
 * @author Sibo Wang
 * @author ra-mit (edits)
 */
package ddprofiler.sources.deprecated;

import java.util.HashMap;
import java.util.Map;

@Deprecated
public class Attribute {

    public enum AttributeType {
        INT, FLOAT, LONG, STRING, UNKNOWN
    }

    public enum AttributeSemanticType {
        SPATIAL, TEMPORAL, NONE
    }


    private String columnName;
    private AttributeType columnType;
    private int columnSize;
    private AttributeSemanticType columnSemanticType;
    private Map<String, String> columnSemanticTypeDetails = new HashMap<>();

    public Attribute(String column_name) {
        this.columnName = column_name;
        this.columnType = AttributeType.UNKNOWN;
        this.columnSize = -1;
    }

    public Attribute(String column_name, String column_type, int column_size) {
        this.columnName = column_name;
        // TODO: swith(column_type) and transform string into enum
        this.columnType = AttributeType.UNKNOWN;
        this.columnSize = column_size;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String column_name) {
        this.columnName = column_name;
    }

    public AttributeType getColumnType() {
        return columnType;
    }

    public void setColumnType(AttributeType column_type) {
        this.columnType = column_type;
    }

    public int getColumnSize() {
        return columnSize;
    }

    public void setColumnSize(int column_size) {
        this.columnSize = column_size;
    }

    public AttributeSemanticType getColumnSemanticType() {
        return columnSemanticType;
    }

    public void setColumnSemanticType(AttributeSemanticType column_semantic_type) {
        this.columnSemanticType = column_semantic_type;
    }

    public Map<String, String> getColumnSemanticTypeDetails() {
        return columnSemanticTypeDetails;
    }

    public String toString() {
        return "column name: " + this.columnName + " column type: " + this.columnType + " column size: "
                + this.columnSize;
    }
}
