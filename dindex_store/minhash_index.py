from dindex_store.common import MinHashIndex
from datasketch import MinHashLSH


class MHIndex(MinHashIndex):

    def __init__(self, threshold, number_permutations, host, port):

        self.threshold = threshold
        self.number_permutations = number_permutations
        self.host = host
        self.port = port

        self.index = MinHashLSH(threshold=0.5, num_perm=128, storage_config={
            'type': 'redis',
            'redis': {'host': host, 'port': port},
        })


if __name__ == "__main__":
    print("Non volatile MinHash Index")
