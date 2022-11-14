from algebra import API
from DoD.utils import FilterType
from DoD import data_processing_utils as dpu
from collections import defaultdict

from api.apiutils import DRS, Operation, OP
from ver_utils.column import Column


class ColumnSelection:
    def __init__(self, network, store_client, csv_separator=","):
        self.aurum_api = API(network=network, store_client=store_client)
        dpu.configure_csv_separator(csv_separator)
        self.topk = 300  # limit the number of columns returned from keyword search

    def column_retreival(self, attr: str, examples: list[str]):
        candidate_columns = {}
        # if attr is given
        if attr != "":
            drs_attr = self.aurum_api.search_exact_attribute(attr, max_results=self.topk)
            if len(drs_attr.data) == 0:
                # if no result is returned, try fuzzy keyword search
                drs_attr = self.aurum_api.search_attribute(attr, max_results=self.topk)
            for x in drs_attr:
                if col.key() not in candidate_columns:
                    col = Column(x.nid, x.source_name, x.field_name)
                    col.hit_type = FilterType.ATTR
                    candidate_columns[col.key()] = col

        # if examples are given
        if len(examples) != 0:
            examples = set(examples) # remove redundant examples
            for example in examples:
                if example == "":
                    continue
                drs_example = self.aurum_api.search_exact_content(example, max_results=self.topk)
                # if no result is returned, try fuzzy search
                if len(drs_example.data) == 0:
                    drs_example = self.aurum_api.search_content(example, max_results=self.topk)
                  
                # 
                drs_example = self.remove_redundant(drs_example, example)
                for x in drs_example:
                    if col.key() not in candidate_columns:
                        col = Column(x.nid, x.source_name, x.field_name)
                    else:
                        col = candidate_columns.get(col.key())
                    col.examples_set.append(x)
                    col.examples_num += 1
                    if col.hit_type == FilterType.ATTR:
                        col.hit_type = FilterType.ATTR_CELL
                    else:
                        col.hit_type = FilterType.CELL
                    candidate_columns[col.key()] = col 
        return candidate_columns

    def cluster_columns(self, candidate_columns):
        candidates = list(candidate_columns.values())
        
        roots = {}
        nid_to_candidate = {}
        cluster = defaultdict(list)
        cluster_score = defaultdict(int)
        for candidate in candidates:
            roots[candidate.nid] = candidate.nid
            nid_to_candidate[candidate.nid] = candidate

        for _, candidate in enumerate(candidates):
            neighbors = self.aurum_api.content_similar_to(candidate)
            for neighbor in neighbors.data:
                if neighbor.nid not in nid_to_candidate:
                    continue
                self.merge_root(roots, candidate.nid, neighbor.nid)
        
        global_max_score = 0

        for candidate in candidates:
            score = candidate.example_num
            cluster_id = self.find_root(roots, candidate.nid)
            cluster[cluster_id].append(candidate)
            cluster_score[cluster_id] = max(cluster_score[cluster_id], score)
            global_max_score = max(global_max_score, score)
        
        result = []
        for cluster_id, score in cluster_score.items():
            if score == global_max_score:
                result.extend(cluster[cluster_id])

        return result
    
    def find_root(self, roots, nid):
        nids = [nid]
        cur = nid
        while roots[cur] != cur:
            cur = roots[cur]
            nids.append(cur)
        for id in nids:
            roots[id] = cur
        return cur
    
    def merge_root(self, roots, nid1, nid2):
        roots[self.find_root(roots, nid1)] = self.find_root(roots, nid2)
    

    @staticmethod
    def remove_redundant(drs_sample, kw):
        lookup = {}
        for drs in drs_sample:
            lookup[drs.nid] = drs
        return DRS(list(lookup.values()), Operation(OP.KW_LOOKUP, params=[kw]))
