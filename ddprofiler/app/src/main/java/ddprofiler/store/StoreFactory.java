package ddprofiler.store;

import ddprofiler.core.config.ProfilerConfig;

public class StoreFactory {

    public static Store makeHttpElasticStore(ProfilerConfig pc) {
        return new HttpElasticStore(pc);
    }

    public static Store makeNativeElasticStore(ProfilerConfig pc) throws Exception {
        throw new Exception("NativeElasticStore is not implemented");
//        return new NativeElasticStore(pc);
    }

    public static Store makeNullStore(ProfilerConfig pc) {
        return new NullStore();
    }

    public static Store makeStoreOfType(int type, ProfilerConfig pc) throws Exception {
        Store s = null;
        if (type == StoreType.NULL.ofType()) {
            s = StoreFactory.makeNullStore(pc);
        } else if (type == StoreType.ELASTIC_HTTP.ofType()) {
            s = StoreFactory.makeHttpElasticStore(pc);
        } else if (type == StoreType.ELASTIC_NATIVE.ofType()) {
            s = StoreFactory.makeNativeElasticStore(pc);
        }
        return s;
    }
}
