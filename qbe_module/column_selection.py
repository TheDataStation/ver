from aurum_api.algebra import AurumAPI
from collections import defaultdict

from aurum_api.apiutils import DRS, Operation, OP
from qbe_module.column import Column
from typing import List
from aurum_api.apiutils import Relation
from enum import Enum
 
class FilterType(Enum):
    ATTR = 1
    CELL = 2
    ATTR_CELL = 3


class ColumnSelection:
    def __init__(self, aurum_api: AurumAPI):
        self.aurum_api = aurum_api
        self.topk = 2000  # limit the number of columns returned from keyword search

    def column_retreival(self, attr: str, examples: List[str]):
        candidate_columns = {}
        # if attr is given
        if attr != "":
            drs_attr = self.aurum_api.search_exact_attribute(attr, max_results=self.topk)
            if len(drs_attr.data) == 0:
                # if no result is returned, try fuzzy keyword search
                drs_attr = self.aurum_api.search_attribute(attr, max_results=self.topk)
            for x in drs_attr:
                if x.nid not in candidate_columns:
                    col = Column(x)
                    col.hit_type = FilterType.ATTR
                    candidate_columns[col.nid] = col
                

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
                  
                drs_example = self.remove_redundant(drs_example, example)
                for x in drs_example:
                    if x.nid not in candidate_columns:
                        col = Column(x)
                        candidate_columns[col.nid] = col
                    else:
                        col = candidate_columns.get(x.nid)
                    col.examples_set.add(example)
                    if col.hit_type == FilterType.ATTR:
                        col.hit_type = FilterType.ATTR_CELL
                    else:
                        col.hit_type = FilterType.CELL
                    candidate_columns[x.nid] = col 
        return candidate_columns

    def cluster_columns(self, candidates: List[Column], prune=True):
        roots = {}
        nid_to_candidate = {}
        cluster = defaultdict(list)
        cluster_score = defaultdict(int)
        for candidate in candidates:
            roots[candidate.nid] = candidate.nid
            nid_to_candidate[candidate.nid] = candidate

        for _, candidate in enumerate(candidates):
            neighbors = self.aurum_api.neighbors(candidate.drs, Relation.CONTENT_SIM)
            for neighbor in neighbors:
                if neighbor.nid not in nid_to_candidate:
                    continue
                self.merge_root(roots, candidate.nid, neighbor.nid)
        
        global_max_score = 0

        for candidate in candidates:
            score = len(candidate.examples_set)
            cluster_id = self.find_root(roots, candidate.nid)
            cluster[cluster_id].append(candidate)
            cluster_score[cluster_id] = max(cluster_score[cluster_id], score)
            global_max_score = max(global_max_score, score)
        
        if prune:
            result = []
            for cluster_id, score in cluster_score.items():
                if score == global_max_score:
                    result.extend(cluster[cluster_id])

            return result
        else:
            return cluster
    
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