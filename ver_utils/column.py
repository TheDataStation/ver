class Column:
    nid = None
    tbl_name = None
    attr_name = None
    example_num = 0 # how many examples the column contains
    examples_set = set() # examples contained in the column
    hit_type = None # ATTR, CELL, or ATTR_CELL 

    def __init__(self, nid, tbl_name, attr_name):
        self.nid = nid
        self.tbl_name = tbl_name
        self.attr_name = attr_name
    
    def key(self):
        return (self.tbl_name, self.attr_name)
    
    def __eq__(self, other):
        if type(other) is type(self):
            return (self.tbl_name == other.tbl_name) and (self.attr_name == other.attr_name)
        else:
            return False

    def __hash__(self):
        return hash((self.tbl_name, self.attr_name))
