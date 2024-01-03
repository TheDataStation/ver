from dindex_store.common import ContentSimilarityIndex
from datasketch import MinHashLSH, MinHash

import numpy as np


class SimpleMHIndex(ContentSimilarityIndex):

    def __init__(self, config, load=False):
        self.threshold = config["minhash_lsh_threshold"]
        self.num_perm = config["minhash_lsh_num_perm"]
        self.index = MinHashLSH(self.threshold, num_perm=self.num_perm)
        if load:
            print("Note that SimpleMHIndex is volatile and so it's not available after loading...")

    def add_profile(self, profile_id, minhash):
        values = np.array(minhash.split(','))
        minhash = MinHash(num_perm=self.num_perm, hashvalues=values)
        self.index.insert(profile_id, minhash)

    def query(self, minhash):
        values = np.array(minhash.split(','))
        minhash = MinHash(num_perm=self.num_perm, hashvalues=values)
        return self.index.query(minhash)


class MHIndex(ContentSimilarityIndex):

    def __init__(self, threshold, number_permutations, host, port):

        self.threshold = threshold
        self.number_permutations = number_permutations
        self.host = host
        self.port = port

        self.index = MinHashLSH(threshold=0.5, num_perm=128, storage_config={
            'type': 'redis',
            'redis': {'host': host, 'port': port},
        })

        raise NotImplementedError


if __name__ == "__main__":
    print("Non volatile MinHash Index")
