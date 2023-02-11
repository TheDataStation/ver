package ddprofiler.store;

import ddprofiler.core.config.ProfilerConfig;

public class StoreFactory {


    public static Store makeElasticStore(ProfilerConfig pc) throws Exception {
        return new ElasticStore(pc);
    }

    public static Store makeJSONFilesStore(ProfilerConfig pc) throws Exception {
        return new JSONFilesStore(pc);
    }

    public static Store makeJSONAndTextFilesStore(ProfilerConfig pc) throws Exception {
        return new JSONProfileAndTextStore(pc);
    }

    public static Store makeNullStore(ProfilerConfig pc) {

        return new NullStore();
    }

    public static Store makeStoreOfType(int type, ProfilerConfig pc) throws Exception {
        Store s = null;
        if (type == StoreType.NULL.ofType()) {
            s = StoreFactory.makeNullStore(pc);
        } else if (type == StoreType.JSON_FILES.ofType()) {
            s = StoreFactory.makeJSONFilesStore(pc);
        }
        else if(type == StoreType.ELASTIC_STORE.ofType()) {
            s = StoreFactory.makeElasticStore(pc);
        }
        else if(type == StoreType.JSON_AND_TEXT_FILES.ofType()) {
            s = StoreFactory.makeJSONAndTextFilesStore(pc);
        }
//        } else if (type == StoreType.ELASTIC_HTTP.ofType()) {
//            s = StoreFactory.makeHttpElasticStore(pc);
//        } else if (type == StoreType.ELASTIC_NATIVE.ofType()) {
//            s = StoreFactory.makeNativeElasticStore(pc);
//        }
        return s;
    }


}
