package ddprofiler.store;

public enum StoreType {
    NULL(0),
    JSON_FILES(1),
    ELASTIC_STORE(2),
    JSON_AND_TEXT_FILES(3);

    private int type;

    StoreType(int type) {
        this.type = type;
    }

    public int ofType() {
        return type;
    }
}
