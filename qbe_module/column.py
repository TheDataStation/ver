from aurum_api.apiutils import DRS

class Column:
    nid = None
    tbl_name = None
    attr_name = None

    def __init__(self, drs: DRS):
        self.drs = drs
        self.nid = drs.nid
        self.tbl_name = drs.source_name
        self.attr_name = drs.field_name
        self.examples_set = set() # examples contained in the column
        self.hit_type = None # ATTR, CELL, or ATTR_CELL 

    def key(self):
        return (self.tbl_name, self.attr_name)
    
    def to_str(self):
        return "{}.{}".format(self.tbl_name, self.attr_name)
    
    def __eq__(self, other):
        if type(other) is type(self):
            return (self.tbl_name == other.tbl_name) and (self.attr_name == other.attr_name)
        else:
            return False

    def __hash__(self):
        return hash((self.tbl_name, self.attr_name))