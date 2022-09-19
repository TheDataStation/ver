package ddprofiler.store;

public enum StoreType {
    NULL(0),
    JSON_FILES(1);
//    ELASTIC_HTTP(1),
//    ELASTIC_NATIVE(2);

    private int type;

    StoreType(int type) {
        this.type = type;
    }

    public int ofType() {
        return type;
    }
}
