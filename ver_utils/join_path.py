from api.apiutils import DRS


class JoinPath:
    def __init__(self, join_key_list: list[DRS]):
        self.join_path = join_key_list

    def to_str(self):
        format_str = ""
        for i, join_key in enumerate(self.join_path):
            format_str += join_key.tbl[:-4] + '.' + join_key.col
            if i < len(self.join_path) - 1:
                format_str += " JOIN "
        return format_str


