class JoinPath:
    def __init__(self, join_key_list):
        self.join_path = join_key_list

    def to_str(self):
        format_str = ""
        for i, join_key in enumerate(self.join_path):
            format_str += join_key.tbl[:-4] + '.' + join_key.col
            if i < len(self.join_path) - 1:
                format_str += " JOIN "
        return format_str

    def print_metadata_str(self):
        print(self.to_str())
        for join_key in self.join_path:
            print(join_key.tbl[:-4] + "." + join_key.col)
            print("datasource: {}, unique_values: {}, non_empty_values: {}, total_values: {}, join_card: {}, jaccard_similarity: {}, jaccard_containment: {}"
            .format(join_key.tbl, join_key.unique_values, join_key.total_values, join_key.non_empty, get_join_type(join_key.join_card), join_key.js, join_key.jc))


class JoinKey:
    def __init__(self, col_drs, unique_values, total_values, non_empty):
        self.tbl = col_drs.source_name
        self.col = col_drs.field_name
        self.unique_values = unique_values
        self.total_values = total_values
        self.non_empty = non_empty
        if col_drs.metadata == 0:
            self.join_card = 0
            self.js = 0
            self.jc = 0
        else:
            self.join_card = col_drs.metadata['join_card']
            self.js = col_drs.metadata['js']
            self.jc = col_drs.metadata['jc']

def get_join_type(join_card):
    if join_card == 0:
        return "One-to-One"
    elif join_card == 1:
        return "One-to-Many"
    elif join_card == 2:
        return "Many-to-One"
    else:
        return "Many-to-Many"
