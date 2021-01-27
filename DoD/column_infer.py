from algebra import API
from DoD.utils import FilterType
from DoD import data_processing_utils as dpu
from api.apiutils import DRS, Operation, OP


class ClusterItem:
    nid = ""
    source_name = ""
    field_name = ""
    tfidf_score = 0
    sample_score = 0
    highlight = []

    def __init__(self, nid, source_name, field_name, tfidf_score, sample_score, highlight):
        self.nid = nid
        self.source_name = source_name
        self.field_name = field_name
        self.tfidf_score = tfidf_score
        self.sample_score = sample_score
        self.highlight = highlight

    def __str__(self):
        return "ClusterItem(nid:%s; source_name:%s; field_name:%s; tfidf_score:%d; sample_score:%d)" % (self.nid, self.source_name, self.field_name, self.tfidf_score, self.sample_score)


class ColumnInfer:

    def __init__(self, network, store_client, csv_separator=","):
        self.aurum_api = API(network=network, store_client=store_client)
        self.paths_cache = dict()
        dpu.configure_csv_separator(csv_separator)
        self.topk = 500  # magic top k number

    """
        Get candidate columns for one attr
        Returns:
        column_candidates: A dictionary of one attr and its candidate columns
        example_hit_dict: A dictionary of one attr and its sample hit number
        hit_type_dict: A dictionary of one attr and its hit type {CELL, ATTR, CELL_AND_ATTR}
    """
    def infer_candidate_columns(self, attrs, values):
        spread_sheet = []
        for i in range(len(attrs)):
            spread_sheet.append((attrs[i], [row[i] for row in values]))

        column_candidates = dict()  # candidate columns for one attr
        column_id = 0
        empty_str = ''

        example_hit_dict = dict()  # dict of column_example_hit. each attribute has a example_hit dict
        hit_type_dict = dict() # dict of hit_type. each attribute has a hit_type dict

        for item in spread_sheet:
            attr = item[0]
            samples = item[1]
            drs_attr = None
            column_example_hit = dict()  # the number of samples contained in one column - column containment score
            hit_type = dict()

            if attr != "":
                drs_attr = self.aurum_api.search_exact_attribute(attr, max_results=self.topk)
                if len(drs_attr.data) == 0:
                    drs_attr = self.aurum_api.search_attribute(attr, max_results=self.topk)
                for x in drs_attr:
                    column_example_hit[(x.source_name, x.field_name)] = 0
                    hit_type[(x.source_name, x.field_name)] = FilterType.ATTR
            drs_samples_union = None
            if empty_str.join(samples) != "":
                # if user provides samples
                drs_union_candidate = []
                for sample in samples:
                    if sample == "":
                        continue
                    drs_sample = self.aurum_api.search_exact_content(sample, max_results=self.topk*3)
                    if len(drs_sample.data) == 0:
                        drs_sample = self.aurum_api.search_content(sample, max_results=self.topk*3)
                    drs_sample_col = self.remove_redundant(drs_sample, sample)
                    for x in drs_sample_col:
                        k = (x.source_name, x.field_name)
                        if k in column_example_hit:
                            column_example_hit[k] = column_example_hit[k] + 1
                            if hit_type[k] == FilterType.ATTR:
                                hit_type[k] = FilterType.ATTR_CELL
                        else:
                            column_example_hit[k] = 1
                            hit_type[k] = FilterType.CELL
                    drs_union_candidate.append(drs_sample)
                drs_samples_union = self.aurum_api.union(drs_union_candidate[0], drs_union_candidate[0])
                for i in range(len(drs_union_candidate)):
                    if i + 1 < len(drs_union_candidate):
                        drs_samples_union = self.aurum_api.union(drs_samples_union, drs_union_candidate[i + 1])

            if attr == "":
                attr = "Column" + str(column_id+1)
            example_hit_dict[attr] = column_example_hit
            hit_type_dict[attr] = hit_type

            if drs_attr is not None and drs_samples_union is not None:
                column_candidates[(attr, FilterType.ATTR_CELL, column_id)] = self.get_intersection(drs_attr, drs_samples_union)
            elif drs_attr is None:
                column_candidates[("Column" + str(column_id+1), FilterType.CELL, column_id)] = drs_samples_union
            else:
                column_candidates[(attr, FilterType.ATTR, column_id)] = drs_attr
            column_id += 1
        return column_candidates, example_hit_dict, hit_type_dict

    """
        Cluster Identical Columns 
        (could be potentially accelerated through hashvalue metadata and column store)
        Returns:
        clusters: A list of ClusterItem
    """
    def cluster_columns(self, candidates, score_map, hit_type):
        lookup = {}
        clusters = []
        for candidate in candidates:
            lookup[candidate] = False
        for candidate in candidates:
            if not lookup[candidate]:
                lookup[candidate] = True
                k = (candidate.source_name, candidate.field_name)
                cluster = [ClusterItem(candidate.nid, candidate.source_name, candidate.field_name, self.get_tfidf_score(candidate, hit_type[k]), score_map[k], candidate.highlight)]
                neighbors = self.aurum_api.content_similar_to(candidate)
                for neighbor in neighbors.data:
                    if neighbor in lookup:
                        k = (neighbor.source_name, neighbor.field_name)
                        cluster.append(ClusterItem(neighbor.nid, neighbor.source_name, neighbor.field_name, self.get_tfidf_score(candidate, hit_type[k]), score_map[k], candidate.highlight))
                        lookup[neighbor] = True
                clusters.append(cluster)
        return clusters

    def get_clusters(self, attrs, values, types):
        candidate_columns, sample_score, hit_type_dict = self.infer_candidate_columns(attrs, values)
        attr_clusters = []
        idx = 0
        for column, candidates in candidate_columns.items():
            clusters = self.cluster_columns(candidates.data, sample_score[column[0]], hit_type_dict[column[0]])
            clusters_list = []
            for cluster in clusters:
                tmp = dict()
                head_values, data_type = self.get_head_values_and_type(cluster[0], 5)
                # if len(head_values) == 0 or data_type.name != types[idx]:
                #     continue       # discard empty columns
                # if len(head_values) == 0:
                #     continue
                tmp["name"] = column[0]
                tmp["sample_score"], max_column = self.get_containment_score(cluster)
                # tmp["data"] = list(map(lambda x: (x.nid, x.source_name, x.field_name, x.tfidf_score), cluster))
                tmp["data"] = list(map(lambda x: (x.nid, x.source_name, x.field_name), cluster))
                tmp["type"] = data_type.name
                tmp["head_values"] = list(set(max_column.highlight)) + head_values
                clusters_list.append(tmp)
            sorted_list = sorted(clusters_list, key=lambda e: e.__getitem__('sample_score'), reverse=True)
            attr_clusters.append(sorted_list)
            idx += 1
        return attr_clusters

    def get_head_values_and_type(self, clusterItem, offset):
        path = self.get_path(clusterItem.nid, clusterItem.source_name)
        df = dpu.read_column(path, clusterItem.field_name, offset)
        top_df = df.dropna().drop_duplicates().values
        return [row[0] for row in top_df], df.dtypes[clusterItem.field_name]

    def get_path(self, nid, table):
        return self.aurum_api.helper.get_path_nid(nid) + table

    @staticmethod
    def get_containment_score(cluster):
        # columns in one cluster should have the same sample containment score
        max_score = -1
        max_column = None
        for column in cluster:
            if column.sample_score > max_score:
                max_score = column.sample_score
                max_column = column
        return max_score, max_column

    @staticmethod
    def correct_cluster_tfidf_score(cluster):
        col_score = {}
        for column in cluster:
            if column.field_name in col_score.keys() and col_score[column.field_name] >= column.tfidf_score:
                continue
            col_score[column.field_name] = column.tfidf_score
        for column in cluster:
            column.tfidf_score = col_score[column.field_name]
        return cluster

    @staticmethod
    def remove_redundant(drs_sample, kw):
        lookup = {}
        for drs in drs_sample:
            k = (drs.source_name, drs.field_name)
            lookup[k] = drs
        return DRS(list(lookup.values()), Operation(OP.KW_LOOKUP, params=[kw]))

    @staticmethod
    def get_tfidf_score(candidate, hit_type):
        if hit_type == FilterType.CELL:
            return 0
        else:
            return round(candidate.score,2)
    
    @staticmethod
    def get_intersection(drs_attr: DRS, drs_sample: DRS) -> DRS:
        lookup = {}
        intersection = []
        if len(drs_attr.data) > len(drs_sample.data):
            lookupDrs = drs_attr
            queryDrs = drs_sample
        else:
            lookupDrs = drs_sample
            queryDrs = drs_attr
        for (idx, hit) in enumerate(lookupDrs.data):
            lookup[(hit.source_name, hit.field_name)] = idx
        for hit in queryDrs.data:
            if (hit.source_name, hit.field_name) in lookup:
                if lookupDrs == drs_sample:
                    intersection.append(lookupDrs.data[lookup[(hit.source_name, hit.field_name)]])
                else:
                    intersection.append(hit)
        return DRS(intersection, Operation(OP.KW_LOOKUP, params=[""]))