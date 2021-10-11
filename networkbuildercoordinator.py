from modelstore.elasticstore import StoreHandler
from knowledgerepr import fieldnetwork
from knowledgerepr import networkbuilder
from knowledgerepr.fieldnetwork import FieldNetwork
from inputoutput import inputoutput as io

import sys
import time


def main(output_path=None, table_path=None):
    start_all = time.time()
    network = FieldNetwork()
    store = StoreHandler()

    # Get all fields from store
    fields_gen = store.get_all_fields()

    # Network skeleton and hierarchical relations (table - field), etc
    start_schema = time.time()
    network.init_meta_schema(fields_gen)
    end_schema = time.time()
    print("Total skeleton: {0}".format(str(end_schema - start_schema)))
    print("!!1 " + str(end_schema - start_schema))

    # # Schema_sim relation
    # start_schema_sim = time.time()
    # schema_sim_index = networkbuilder.build_schema_sim_relation(network)
    # end_schema_sim = time.time()
    # print("Total schema-sim: {0}".format(str(end_schema_sim - start_schema_sim)))
    # print("!!2 " + str(end_schema_sim - start_schema_sim))

    # # Entity_sim relation
    # start_entity_sim = time.time()
    # #fields, entities = store.get_all_fields_entities()
    # #networkbuilder.build_entity_sim_relation(network, fields, entities)
    # end_entity_sim = time.time()
    # print("Total entity-sim: {0}".format(str(end_entity_sim - start_entity_sim)))

    # Content_sim text relation (minhash-based)
    start_text_sig_sim = time.time()
    st = time.time()
    mh_signatures = store.get_all_mh_text_signatures()
    et = time.time()
    print("Time to extract minhash signatures from store: {0}".format(str(et - st)))
    print("!!3 " + str(et - st))

    print("Begin to extract text-sim")
    lsh_threshold = 0.5
    content_sim_index, empty_cnt, edges_cnt, failed_cnt = networkbuilder.build_content_sim_mh_text_js(network, mh_signatures, lsh_threshold, table_path)
    end_text_sig_sim = time.time()
    print("Total text-sig-sim (minhash): {0}; empty column names: {1}; edges count: {2}; failed count: {3}".format(str(end_text_sig_sim - start_text_sig_sim), str(empty_cnt), str(edges_cnt), str(failed_cnt)))
    print("!!4 " + str(end_text_sig_sim - start_text_sig_sim))

    # Content_sim num relation
    start_num_sig_sim = time.time()
    id_sig = store.get_all_fields_num_signatures()
    #networkbuilder.build_content_sim_relation_num(network, id_sig)
    networkbuilder.build_content_sim_relation_num_overlap_distr(network, id_sig, table_path)
    #networkbuilder.build_content_sim_relation_num_overlap_distr_indexed(network, id_sig)
    end_num_sig_sim = time.time()
    print("Total num-sig-sim: {0}".format(str(end_num_sig_sim - start_num_sig_sim)))
    print("!!5 " + str(end_num_sig_sim - start_num_sig_sim))

    # Primary Key / Foreign key relation
    # start_pkfk = time.time()
    # networkbuilder.build_pkfk_relation(network)
    # end_pkfk = time.time()
    # print("Total PKFK: {0}".format(str(end_pkfk - start_pkfk)))
    # print("!!6 " + str(end_pkfk - start_pkfk))

    end_all = time.time()
    print("Total time: {0}".format(str(end_all - start_all)))
    print("!!7 " + str(end_all - start_all))

    path = "test/datagov/"
    if output_path is not None:
        path = output_path
    fieldnetwork.serialize_network(network, path)

    # Serialize indexes
    # path_schsim = path + "/schema_sim_index.pkl"
    # io.serialize_object(schema_sim_index, path_schsim)
    path_cntsim = path + "/content_sim_index.pkl"
    io.serialize_object(content_sim_index, path_cntsim)

    print("DONE!")


def plot_num():
    network = FieldNetwork()
    store = StoreHandler()
    fields, num_signatures = store.get_all_fields_num_signatures()

    xaxis = []
    yaxis = []
    numpoints = 0
    for x, y in num_signatures:
        numpoints = numpoints + 1
        xaxis.append(x)
        yaxis.append(y)
    print("Num points: " + str(numpoints))
    import matplotlib.pyplot as plt
    plt.plot(xaxis, yaxis, 'ro')
    plt.axis([0, 600000, 0, 600000])
    #plt.axis([0, 10000, 0, 10000])
    #plt.axis([0, 500, 0, 500])
    plt.show()


def test_content_sim_num():

    '''
    SETUP
    '''

    start_all = time.time()
    network = FieldNetwork()
    store = StoreHandler()

    # Get all fields from store
    fields_gen = store.get_all_fields()

    # Network skeleton and hierarchical relations (table - field), etc
    start_schema = time.time()
    network.init_meta_schema(fields_gen)
    end_schema = time.time()
    print("Total skeleton: {0}".format(str(end_schema - start_schema)))

    '''
    ACTUAL TEST
    '''

    # Content_sim num relation
    start_num_sig_sim = time.time()
    id_sig = store.get_all_fields_num_signatures()
    # networkbuilder.build_content_sim_relation_num(network, id_sig)
    networkbuilder.build_content_sim_relation_num_overlap_distr(network, id_sig)
    end_num_sig_sim = time.time()
    print("Total num-sig-sim: {0}".format(str(end_num_sig_sim - start_num_sig_sim)))


if __name__ == "__main__":

    #test_content_sim_num()
    #exit()

    path = None
    if len(sys.argv) == 5:
        path = sys.argv[2]
        table_path = sys.argv[4]

    else:
        print("USAGE: ")
        print("python networkbuildercoordinator.py --opath <model_path> --tpath <table_path>")
        print("where opath must be writable by the process")
        exit()
    main(path, table_path)

    #test_read_store()

    #test()
    #plot_num()
    #test_cardinality_propagation()
