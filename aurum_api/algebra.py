import itertools
from enum import Enum
import argparse

from aurum_api.apiutils import compute_field_id as id_from
from aurum_api.apiutils import Operation
from aurum_api.apiutils import OP
from aurum_api.apiutils import Relation
from aurum_api.apiutils import DRS
from aurum_api.apiutils import DRSMode
from aurum_api.apiutils import Hit


from dindex_store.discovery_index import DiscoveryIndex


class KWType(Enum):
    KW_CONTENT = 0
    KW_SCHEMA = 1
    KW_ENTITIES = 2
    KW_TABLE = 3
    KW_METADATA = 4


class Algebra:

    def __init__(self, dindex: DiscoveryIndex):
        self.dindex = dindex

    """
    Basic API
    """

    def search(self, kw: str, kw_type=KWType.KW_CONTENT, max_results=10) -> DRS:
        """
        Performs a keyword search over the contents of the data.
        Scope specifies where elasticsearch should be looking for matches.
        i.e. table titles (SOURCE), columns (FIELD), or comment (SOURCE)

        :param kw: the keyword to serch
        :param kw_type: the context type on which to search
        :param max_results: maximum number of results to return
        :return: returns a DRS
        """

        results = self.dindex.fts_query(keywords=kw, search_domain=kw_type, max_results=max_results, exact_search=False)
        hits = [Hit(r[0], r[1], r[3], r[4], r[5], []) for r in results]

        # data = Hit(str(el['_source']['id']), el['_source']['dbName'], el['_source']['sourceName'],
        #            el['_source']['columnName'], el['_score'], matched_text)
        # hits = self._store_client.search_keywords(
        #     keywords=kw, elasticfieldname=kw_type, max_hits=max_results)

        # materialize generator
        drs = DRS([x for x in hits], Operation(OP.KW_LOOKUP, params=[kw]))
        return drs

    def exact_search(self, kw: str, kw_type: KWType, max_results=10):
        """
        See 'search'. This only returns exact matches.
        """

        results = self.dindex.fts_query(keywords=kw, search_domain=kw_type, max_results=max_results, exact_search=True)
        
        # hits = [Hit(r.nid, r.db_name, r.s_name, r.f_name, 0, []) for r in results]
        hits = [Hit(r[0], r[1], r[3], r[4], 0, []) for r in results]
        # hits = self._store_client.exact_search_keywords(
        #     keywords=kw, elasticfieldname=kw_type, max_hits=max_results)

        # materialize generator
        drs = DRS([x for x in hits], Operation(OP.KW_LOOKUP, params=[kw]))
        return drs

    def search_content(self, kw: str, max_results=10) -> DRS:
        return self.search(kw, kw_type=KWType.KW_CONTENT, max_results=max_results)

    def search_exact_content(self, kw: str, max_results=10) -> DRS:
        return self.exact_search(kw, kw_type=KWType.KW_CONTENT, max_results=max_results)

    def search_attribute(self, kw: str, max_results=10) -> DRS:
        return self.search(kw, kw_type=KWType.KW_SCHEMA, max_results=max_results)

    def search_exact_attribute(self, kw: str, max_results=10) -> DRS:
        return self.exact_search(kw, kw_type=KWType.KW_SCHEMA, max_results=max_results)

    def search_table(self, kw: str, max_results=10) -> DRS:
        return self.search(kw, kw_type=KWType.KW_TABLE, max_results=max_results)

    def suggest_schema(self, kw: str, max_results=5):
        # TODO: suggest_schema, when implemented on elastic, used to solve 'autocompletion'. This is lost on duckdb
        # return self._store_client.suggest_schema(kw, max_hits=max_results)
        raise NotImplementedError

    def __neighbor_search(self,
                        input_data,
                        relation: Relation):
        """
        Given an nid, node, hit or DRS, finds neighbors with specified
        relation.
        :param nid, node tuple, Hit, or DRS:
        """
        # convert whatever input to a DRS
        i_drs = self._general_to_drs(input_data)

        # prepare an output DRS
        o_drs = DRS([], Operation(OP.NONE))
        o_drs = o_drs.absorb_provenance(i_drs)

        # get all of the table Hits in a DRS, if necessary.
        if i_drs.mode == DRSMode.TABLE:
            self._general_to_field_drs(i_drs)

        # Check neighbors
        if not relation.from_metadata():
            for h in i_drs:
                attrs = ["id, dbName, sourceName, columnName"]
                results = self.dindex.find_neighborhood(h.nid, relation, hops=1, desired_attributes=attrs)
                
                # hits = [Hit(r.id, r.db_name, r.s_name, r.f_name, 0, []) for r in results]
                # make sure neighbors are from different tables with the input
                hits = [Hit(r[0], r[1], r[2], r[3], 0, []) for r in results if r[2] != h.source_name]

                # # FIXME: adapt results from index to API consumable DRS objects
                # hits_drs = self._network.neighbors_id(h, relation)

                op = relation.get_op_from_relation(relation)
                hits_drs = DRS(hits, Operation(op, params=[h]))

                o_drs = o_drs.absorb(hits_drs)
        else:
            # TODO: refactor/redesign metadata as needed (factor it out of algebra)
            raise NotImplementedError
            # md_relation = self._relation_to_mdrelation(relation)
            # for h in i_drs:
            #     neighbors = self.md_search(h, md_relation)
            #     hits_drs = self._network.md_neighbors_id(h, neighbors, relation)
            #     o_drs = o_drs.absorb(hits_drs)
        return o_drs

    def content_similar_to(self, general_input):
        return self.__neighbor_search(input_data=general_input, relation=Relation.CONTENT_SIM)

    def schema_similar_to(self, general_input):
        return self.__neighbor_search(input_data=general_input, relation=Relation.SCHEMA_SIM)

    def pkfk_of(self, general_input):
        return self.__neighbor_search(input_data=general_input, relation=Relation.PKFK)

    """
    TC API
    """

    def neighbors(self, drs: DRS, relation):
        return self.__neighbor_search(drs, relation)

    def paths(self, drs_a: DRS, drs_b: DRS, relation=Relation.PKFK, max_hops=2, lean_search=False) -> DRS:
        """
        Is there a transitive relationship between any element in a with any
        element in b?
        This function finds the answer constrained on the primitive
        (singular for now) that is passed as a parameter.
        If b is not passed, assumes the user is searching for paths between
        elements in a.
        :param a: DRS
        :param b: DRS
        :param Relation: Relation
        :return:
        """
        # FIXME: paths are not adapted to the new below-aurum; needs to be redesigned (march'23)
        raise NotImplementedError
        # create b if it wasn't passed in.
        drs_a = self._general_to_drs(drs_a)
        drs_b = self._general_to_drs(drs_b)

        self._assert_same_mode(drs_a, drs_b)

        # absorb the provenance of both a and b
        o_drs = DRS([], Operation(OP.NONE))
        o_drs.absorb_provenance(drs_a)
        if drs_b != drs_a:
            o_drs.absorb_provenance(drs_b)

        for h1, h2 in itertools.product(drs_a, drs_b):

            # there are different network operations for table and field mode
            res_drs = None
            if drs_a.mode == DRSMode.FIELDS:
                res_drs = self._network.find_path_hit(
                    h1, h2, relation, max_hops=max_hops)
            else:
                res_drs = self._network.find_path_table(
                    h1, h2, relation, self, max_hops=max_hops, lean_search=lean_search)

            o_drs = o_drs.absorb(res_drs)

        return o_drs

    """
    Combiner API
    """

    def intersection(self, a: DRS, b: DRS) -> DRS:
        """
        Returns elements that are both in a and b
        :param a: an iterable object
        :param b: another iterable object
        :return: the intersection of the two provided iterable objects
        """
        a = self._general_to_drs(a)
        b = self._general_to_drs(b)
        self._assert_same_mode(a, b)

        o_drs = a.intersection(b)
        return o_drs

    def union(self, a: DRS, b: DRS) -> DRS:
        """
        Returns elements that are in either a or b
        :param a: an iterable object
        :param b: another iterable object
        :return: the union of the two provided iterable objects
        """
        a = self._general_to_drs(a)
        b = self._general_to_drs(b)
        self._assert_same_mode(a, b)

        o_drs = a.union(b)
        return o_drs

    def difference(self, a: DRS, b: DRS) -> DRS:
        a = self._general_to_drs(a)
        b = self._general_to_drs(b)
        """
        Returns elements that are in either a or b
        :param a: an iterable object
        :param b: another iterable object
        :return: the union of the two provided iterable objects
        """
        a = self._general_to_drs(a)
        b = self._general_to_drs(b)
        self._assert_same_mode(a, b)

        o_drs = a.set_difference(b)
        return o_drs

    """
    Helper Functions
    """
    def nid_to_drs(self, nid) -> DRS:
        hit = self._nid_to_hit(nid)
        return self._hit_to_drs(hit)

    def make_drs(self, general_input):
        """
        Makes a DRS from general_input.
        general_input can include an array of strings, Hits, DRS's, etc,
        or just a single DRS.
        """
        try:

            # If this is a list of inputs, condense it into a single drs
            if isinstance(general_input, list):
                general_input = [
                    self._general_to_drs(x) for x in general_input]

                combined_drs = DRS([], Operation(OP.NONE))
                for drs in general_input:
                    combined_drs = self.union(combined_drs, drs)
                general_input = combined_drs

            # else, just convert it to a DRS
            o_drs = self._general_to_drs(general_input)
            return o_drs
        except:
            msg = (
                '--- Error ---' +
                '\nThis function returns domain result set from the ' +
                'supplied input' +
                '\nusage:\n\tmake_drs( table name/hit id | [table name/hit ' +
                'id, drs/hit/string/int] )' +
                '\ne.g.:\n\tmake_drs(1600820766)')
            print(msg)

    def _general_to_drs(self, general_input) -> DRS:
        """
        Given an nid, node, hit, or DRS and convert it to a DRS.
        :param nid: int
        :param node: (db_name, source_name, field_name)
        :param hit: Hit
        :param DRS: DRS
        :return: DRS
        """
        # test for DRS initially for speed
        if isinstance(general_input, DRS):
            return general_input

        if general_input is None:
            general_input = DRS(data=[], operation=Operation(OP.NONE))

        # Test for ints or strings that represent integers
        if self._represents_int(general_input):
            general_input = self._nid_to_hit(general_input)

        # Test for strings that represent tables
        if isinstance(general_input, str):
            results = self.dindex.get_filtered_profiles_from_table(general_input, ['id', 'dbname', 'sourcename', 'columnname'])
            hits = [Hit(r[0], r[1], r[2], r[3], 0, []) for r in results]
            general_input = DRS([x for x in hits], Operation(OP.ORIGIN))

        # Test for tuples that are not Hits
        if (isinstance(general_input, tuple) and
                not isinstance(general_input, Hit)):
            general_input = self._node_to_hit(general_input)

        # Test for Hits
        if isinstance(general_input, Hit):
            field = general_input.field_name
            if field == '' or field is None:
                # If the Hit's field is not defined, it is in table mode
                # and all Hits from the table need to be found
                general_input = self._hit_to_drs(general_input, table_mode=True)
            else:
                general_input = self._hit_to_drs(general_input)
        if isinstance(general_input, DRS):
            return general_input

        raise ValueError(
            'Input is not None, an integer, field tuple, Hit, or DRS')

    def _nid_to_hit(self, nid: int) -> Hit:
        """
        Given a node id, convert it to a Hit
        :param nid: int or string
        :return: DRS
        """
        nid = str(nid)
        score = 0.0

        results = self.dindex.get_filtered_profiles_from_nids([nid], ['id', 'dbname', 'sourcename', 'columnname'])
        hit = [Hit(r[0], r[1], r[2], r[3], 0, []) for r in results][0]

        # nid, db, source, field = self._network.get_info_for([nid])[0]
        # hit = Hit(nid, db, source, field, score,[])

        return hit

    def _node_to_hit(self, node: (str, str, str)) -> Hit:
        """
        Given a field and source name, it returns a Hit with its representation
        :param node: a tuple with the name of the field,
            (db_name, source_name, field_name)
        :return: Hit
        """
        db, source, field = node
        nid = id_from(db, source, field)
        hit = Hit(nid, db, source, field, 0, [])
        return hit

    def hits_to_drs(self, hits):
        return DRS(hits, Operation(OP.ORIGIN))

    def _hit_to_drs(self, hit: Hit, table_mode=False) -> DRS:
        """
        Given a Hit, return a DRS. If in table mode, the resulting DRS will
        contain Hits representing that table.
        :param hit: Hit
        :param table_mode: if the Hit represents an entire table
        :return: DRS
        """
        drs = None
        if table_mode:
            table_name = hit.source_name
            results = self.dindex.get_filtered_profiles_from_table(table_name, ['id', 'dbname', 'sourcename', 'columnname'])
            hits = [Hit(r[0], r[1], r[2], r[3], 0, []) for r in results]
            drs = DRS([x for x in hits], Operation(OP.TABLE, params=[hit]))
            drs.set_table_mode()
        else:
            drs = DRS([hit], Operation(OP.ORIGIN))

        return drs

    def _general_to_field_drs(self, general_input):
        drs = self._general_to_drs(general_input)

        drs.set_fields_mode()
        for h in drs:
            fields_table = self._hit_to_drs(h, table_mode=True)
            drs = drs.absorb(fields_table)

        return drs

    def _assert_same_mode(self, a: DRS, b: DRS) -> None:
        error_text = ("Input parameters are not in the same mode ",
                      "(fields, table)")
        assert a.mode == b.mode, error_text

    def _represents_int(self, string: str) -> bool:
        try:
            int(string)
            return True
        except:
            return False

    """
    Helper API
    """

    def help(self):
        """
        Prints general help information, or specific usage information of a function if provided
        :param function: an optional function
        """
        from IPython.display import Markdown, display

        def print_md(string):
            display(Markdown(string))

        # Check whether the request is for some specific function
        #if function is not None:
        #    print_md(self.function.__doc__)
        # If not then offer the general help menu
        #else:
        print_md("### Help Menu")
        print_md("You can use the system through an **API** object. API objects are returned"
                 "by the *init_system* function, so you can get one by doing:")
        print_md("***your_api_object = init_system('path_to_stored_model')***")
        print_md("Once you have access to an API object there are a few concepts that are useful "
                 "to use the API. **content** refers to actual values of a given field. For "
                 "example, if you have a table with an attribute called __Name__ and values *Olu, Mike, Sam*, content "
                 "refers to the actual values, e.g. Mike, Sam, Olu.")
        print_md("**schema** refers to the name of a given field. In the previous example, schema refers to the word"
                 "__Name__ as that's how the field is called.")
        print_md("Finally, **entity** refers to the *semantic type* of the content. This is in experimental state. For "
                 "the previous example it would return *'person'* as that's what those names refer to.")
        print_md("Certain functions require a *field* as input. In general a field is specified by the source name ("
                 "e.g. table name) and the field name (e.g. attribute name). For example, if we are interested in "
                 "finding content similar to the one of the attribute *year* in the table *Employee* we can provide "
                 "the field in the following way:")
        print(
            "field = ('Employee', 'year') # field = [<source_name>, <field_name>)")


class AurumAPI(Algebra):
    def __init__(self, *args, **kwargs):
        super(AurumAPI, self).__init__(*args, **kwargs)


def main(dindex_path, interactive=False, create_reporting=False):
    from IPython.display import Markdown, display
    from IPython.terminal.embed import InteractiveShellEmbed

    from aurum_api.reporting import Report
    from dindex_builder.dindex_builder import load_dindex

    import time

    def print_md(string):
        display(Markdown(string))

    print_md("Loading DIndex")
    sl = time.time()

    import config
    cnf = {setting: getattr(config, setting) for setting in dir(config)
           if setting.islower() and len(setting) > 2 and setting[:2] != "__"}

    dindex = load_dindex(cnf)
    print_md("Loading DIndex...OK")

    api = AurumAPI(dindex)
    if create_reporting:
        reporting = Report(dindex)
        # TODO: need to refactor report to use dindex
        raise NotImplementedError
    else:
        # Better always return a tuple even if second element is `None`
        reporting = None
    api.help()
    el = time.time()
    print("Took " + str(el - sl) + " to load model")
    if interactive:
        init_banner = "Welcome to Ver (Aurum API). \nYou can access the API via the object api"
        exit_banner = "Bye!"
        ip_shell = InteractiveShellEmbed(banner1=init_banner, exit_msg=exit_banner)
        ip_shell()
    return api, reporting


if __name__ == '__main__':

    print("Aurum API")

    # FIXME: add correct list of params
    # Argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--dindex_path', help='Path to discovery index')
    parser.add_argument('--interactive', default=False, help='embed api into an iPython session')
    parser.add_argument('--create_reporting', default=False, help='create a reporting API (to inspect dindex)')

    args = parser.parse_args()

    api, reporting = main(args.dindex_path, interactive=args.interactive, create_reporting=args.create_reporting)

    res = api.search("madden")

    print(res.pretty_print_columns())
    print(res.print_columns())
    # exit()

    print("Test 1")
    results = api.dindex.get_filtered_profiles_from_nids([316673731, 263143580], ['id', 'dbname', 'sourcename', 'columnname'])
    hits = [Hit(r[0], r[1], r[2], r[3], 0, []) for r in results]

    print("Test 2")
    results = api.dindex.get_filtered_profiles_from_nids([316673731], ['id', 'dbname', 'sourcename', 'columnname'])
    hit = [Hit(r[0], r[1], r[2], r[3], 0, []) for r in results][0]
    print(hit)

    print("Test 3")
    results = api.dindex.get_filtered_profiles_from_table('Drupal_employee_directory.csv', ['id', 'dbname', 'sourcename', 'columnname'])
    hits = [Hit(r[0], r[1], r[2], r[3], 0, []) for r in results]
    print(hits)

