'''
Algorithm:

Input: a list of signals, views

1. Evaluate each signal, if necessary (Some signals may only needed to be evaluated one time or lazily) (deterministic)
2. Split the views according to the signals (deterministic?) (try hierarchical clustering?)
3. Pick the best signal to split on according to the impurity criterion
4. Show user the representative view (function) from each split (each group of views)
5. User assigns a score to each representative view (or rank them in order of preference).
6. Update the ranking model.
7. Repeat from 3. Pick the next best signal to split on (with replacement if a signal is continuous or
   without replacement if a signal is categorical). The process continues until there’s no more signal to split on
   or user is satisfied with the current result
8. Show the user the views in the order of their preference, based on a ranking criterion.

'''
from abc import ABC, abstractmethod
import random


class Signal(ABC):

    @abstractmethod
    def evaluate(self):
        pass

    @abstractmethod
    def representatives(self):
        # returns {'signal_value': representative sample to show user}
        pass


class ContinuousSignal(Signal):

    @abstractmethod
    def cluster(self, threshold):
        pass

    @abstractmethod
    def split(self, threshold, cluster=None):
        # Can re-split on a specific cluster
        pass


class DiscreteSignal(Signal):

    @abstractmethod
    def split(self):
        pass


class ViewSize(ContinuousSignal):

    def __init__(self, views):
        self.views = views
        self.sizes = {}  # sizes[size] = [views]
        self.clusters = {}  # clusters[label] = [views]

    def evaluate(self):
        self.sizes.clear()
        for df_file_tuple in self.views:
            view = df_file_tuple[0]
            if len(view) in self.sizes.keys():
                self.sizes[len(view)].append(df_file_tuple)
            else:
                self.sizes[len(view)] = [df_file_tuple]

    def cluster(self, threshold):
        # TODO
        from sklearn.cluster import KMeans
        import numpy as np

        to_cluster = np.array(list(self.sizes.keys()), dtype=int).reshape(-1, 1)
        kmeans = KMeans(n_clusters=threshold).fit(to_cluster)

        labels = kmeans.labels_.tolist()
        sizes = to_cluster.flatten().tolist()
        assert len(labels) == len(sizes)

        self.clusters.clear()
        # views with the same label belong in same cluster
        for label, size in zip(labels, sizes):
            if label in self.clusters.keys():
                self.clusters[label] = self.clusters[label] + self.sizes[size]
            else:
                self.clusters[label] = self.sizes[size]

    def split(self, threshold, cluster=None):
        if cluster != None:
            # Narrowing down the region
            self.views = self.clusters[cluster]
            self.evaluate()

        if len(self.sizes.keys()) > threshold:
            self.cluster(threshold)
        else:
            self.clusters = self.sizes
        return self.clusters

    def representatives(self):
        # TODO: choose a random view in each group for now
        representatives = {}
        for label, views in self.clusters.items():
            representatives[label] = random.choice(views)
        return representatives


class PrimaryKey(DiscreteSignal):

    def __init__(self, dict):
        # dict['pk'] = [views]
        self.primary_keys = dict

    def evaluate(self):
        pass

    def split(self):
        return self.primary_keys

    def representatives(self):
        # TODO: choose a random view in each group for now
        representatives = {}
        for pk, views in self.primary_keys.items():
            representatives[pk] = random.choice(views)
        return representatives


def pick_best_signal_to_split(splits):
    # If the user randomly pick a branch in the split, what's the expected value of uncertainty removed?
    # ev = sum over p_i * x+i
    # gain = total size - ev
    # TODO: Do we need to use gain ratio to penalize large number of distinct values, or it's unnecessary
    #  if we already have the split threshold?
    max_gain = -1
    for signal, dict in splits.items():
        split = [len(views) for views in dict.values()]
        expected_value = 0.0
        split_size = sum(split)
        for num in split:
            expected_value += num / split_size * num
        gain = split_size - expected_value
        if gain > max_gain:
            best_signal = signal
    return best_signal


if __name__ == '__main__':

    import glob
    import pandas as pd
    import pprint

    dir_path = "./test"
    cvs_files = glob.glob(dir_path + "/view_*")
    view_dfs = []
    for f in cvs_files:
        df = pd.read_csv(f)
        view_dfs.append((df, f))

    signals = []

    size_signal = ViewSize(view_dfs)
    signals.append(size_signal)

    # assign fake primary keys randomly
    pk_dict = {}
    random.shuffle(view_dfs)
    fake_pk_dict = {}
    num_pks = 2
    for i in range(num_pks):
        fake_pk_dict[i] = view_dfs[i::num_pks]
    pk_signal = PrimaryKey(fake_pk_dict)
    signals.append(pk_signal)

    # Evaluate
    for signal in signals:
        signal.evaluate()

    # Do split (and do clustering based on split_threshold if necessary)
    split_threshold = 3
    splits = {}
    for signal in signals:
        if isinstance(signal, ContinuousSignal):
            splits[signal] = signal.split(threshold=split_threshold)
        if isinstance(signal, DiscreteSignal):
            splits[signal] = signal.split()

    # Initialize ranking model, with the score of each view = 0
    ranking_model = {}
    for df_file_tuple in view_dfs:
        view_file = df_file_tuple[1]  # can't hash dataframe, so only hash filename
        ranking_model[view_file] = 0

    num_iters = 0
    while num_iters < 5 and len(splits) > 0:
        best_signal = pick_best_signal_to_split(splits)
        print("Best signal to split: ", best_signal.__class__.__name__)

        representatives = best_signal.representatives()

        # skipping user interaction part...
        print("Pick your preferred view:")
        pprint.pprint(representatives)

        # user randomly picks one branch from the representatives
        random_pick = random.choice(list(representatives.keys()))
        print("User chooses ", random_pick, "\n")

        # TODO: let user assign their own score to each representative instead of picking just one, and update
        #  ranking model accordingly

        # +1 to every view in the chosen group / cluster
        for df_file_tuple in splits[best_signal][random_pick]:
            view_file = df_file_tuple[1]
            ranking_model[view_file] += 1

        if isinstance(best_signal, ContinuousSignal):
            # Continue splitting on the cluster chosen
            splits[best_signal] = best_signal.split(threshold=split_threshold, cluster=random_pick)
            if len(splits[best_signal]) == 0:
                splits.pop(best_signal)
        if isinstance(best_signal, DiscreteSignal):
            # Delete the signal so we won't choose from it again
            splits.pop(best_signal)

        num_iters += 1

    # present the list of views ordered by accumulated score
    sorted_rank = [(view, score) for view, score in
                   sorted(ranking_model.items(), key=lambda item: item[1], reverse=True)]
    print("Views ordered by preference score:")
    pprint.pprint(sorted_rank)
