from aurum_api.annotation import MDClass
from aurum_api.annotation import MDRelation
from aurum_api.annotation import MRS
from aurum_api.apiutils import Relation
from aurum_api.apiutils import DRSMode

from dindex_store.discovery_index import DiscoveryIndex


class Catalog:

    def __init__(self, dindex: DiscoveryIndex):
        self.dindex = dindex

    """
    OLD functions from ddapi -> algebra
    We may want to revisit these, but the idea of having a catalog separated from algebra is good, both 
    feeding off the DiscoveryIndex
    """
    # FIXME: needs to be adapted to dindex, if we want to keep them

    def _mdclass_to_str(self, md_class: MDClass):
        ref_table = {
            MDClass.WARNING: "warning",
            MDClass.INSIGHT: "insight",
            MDClass.QUESTION: "question"
        }
        return ref_table[md_class]

    def _mdrelation_to_str(self, md_relation: MDRelation):
        """
        :return: (str, nid_is_source)
        """
        ref_table = {
            MDRelation.MEANS_SAME_AS: ("same", True),
            MDRelation.MEANS_DIFF_FROM: ("different", True),
            MDRelation.IS_SUBCLASS_OF: ("subclass", True),
            MDRelation.IS_SUPERCLASS_OF: ("subclass", False),
            MDRelation.IS_MEMBER_OF: ("member", True),
            MDRelation.IS_CONTAINER_OF: ("member", False)
        }
        return ref_table[md_relation]

    def _relation_to_mdrelation(self, relation):
        if relation == Relation.MEANS_SAME:
            return MDRelation.MEANS_SAME_AS
        if relation == Relation.MEANS_DIFF:
            return MDRelation.MEANS_DIFF_FROM
        if relation == Relation.SUBCLASS:
            return MDRelation.IS_SUBCLASS_OF
        if relation == Relation.SUPERCLASS:
            return MDRelation.IS_SUPERCLASS_OF
        if relation == Relation.MEMBER:
            return MDRelation.IS_MEMBER_OF
        if relation == Relation.CONTAINER:
            return MDRelation.IS_CONTAINER_OF

    # Hide these for the time-being

    def __annotate(self, author: str, text: str, md_class: MDClass,
                   general_source, ref={"general_target": None, "type": None}) -> MRS:
        """
        Create a new annotation in the elasticsearch graph.
        :param author: identifiable name of user or process
        :param text: free text description
        :param md_class: MDClass
        :param general_source: nid, node tuple, Hit, or DRS
        :param ref: (optional) {
            "general_target": nid, node tuple, Hit, or DRS,
            "type": MDRelation
        }
        :return: MRS of the new metadata
        """
        source = self._general_to_drs(general_source)
        target = self._general_to_drs(ref["general_target"])

        if source.mode != DRSMode.FIELDS or target.mode != DRSMode.FIELDS:
            raise ValueError("source and targets must be columns")

        md_class = self._mdclass_to_str(md_class)
        md_hits = []

        # non-relational metadata
        if ref["type"] is None:
            for hit_source in source:
                res = self._store_client.add_annotation(
                    author=author,
                    text=text,
                    md_class=md_class,
                    source=hit_source.nid)
                md_hits.append(res)
            return MRS(md_hits)

        # relational metadata
        md_relation, nid_is_source = self._mdrelation_to_str(ref["type"])
        if not nid_is_source:
            source, target = target, source

        for hit_source in source:
            for hit_target in target:
                res = self._store_client.add_annotation(
                    author=author,
                    text=text,
                    md_class=md_class,
                    source=hit_source.nid,
                    target={"id": hit_target.nid, "type": md_relation})
                md_hits.append(res)
            return MRS(md_hits)

    def __add_comments(self, author: str, comments: list, md_id: str) -> MRS:
        """
        Add comments to the annotation with the given md_id.
        :param author: identifiable name of user or process
        :param comments: list of free text comments
        :param md_id: metadata id
        """
        md_comments = []
        for comment in comments:
            res = self._store_client.add_comment(
                author=author, text=comment, md_id=md_id)
            md_comments.append(res)
        return MRS(md_comments)

    def __add_tags(self, author: str, tags: list, md_id: str):
        """
        Add tags/keywords to metadata with the given md_id.
        :param md_id: metadata id
        :param tags: a list of tags to add
        """
        return self._store_client.add_tags(author, tags, md_id)

    def __md_search(self, general_input=None,
                    relation: MDRelation = None) -> MRS:
        """
        Searches for metadata that reference the nodes in the general
        input. If a relation is given, searches for metadata that mention the
        nodes as the source of the relation. If no parameters are given,
        searches for all metadata.
        :param general_input: nid, node tuple, Hit, or DRS
        :param relation: an MDRelation
        """
        # return all metadata
        if general_input is None:
            return MRS([x for x in self._store_client.get_metadata()])

        drs_nodes = self._general_to_drs(general_input)
        if drs_nodes.mode != DRSMode.FIELDS:
            raise ValueError("general_input must be columns")

        # return metadata that reference the input
        if relation is None:
            md_hits = []
            for node in drs_nodes:
                md_hits.extend(self._store_client.get_metadata(nid=node.nid))
            return MRS(md_hits)

        # return metadata that reference the input with the given relation
        md_hits = []
        store_relation, nid_is_source = self._mdrelation_to_str(relation)
        for node in drs_nodes:
            md_hits.extend(self._store_client.get_metadata(nid=node.nid,
                                                           relation=store_relation, nid_is_source=nid_is_source))
        return MRS(md_hits)

    def __md_keyword_search(self, kw: str, max_results=10) -> MRS:
        """
        Performs a keyword search over metadata annotations and comments.
        :param kw: the keyword to search
        :param max_results: maximum number of results to return
        :return: returns a MRS
        """
        hits = self._store_client.search_keywords_md(
            keywords=kw, max_hits=max_results)

        mrs = MRS([x for x in hits])
        return mrs


class CatalogAPI(Catalog):
    def __init__(self, *args, **kwargs):
        super(CatalogAPI, self).__init__(*args, **kwargs)

    # FIXME: we could follow the strategy to initialize this API that algebra follows, once we like how it works