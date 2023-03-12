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
from archived.DoD import view_4c_analysis_baseline as v4c


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
        # returns {group1: [view_files], group2: [view_files], ...}
        pass


class DiscreteSignal(Signal):

    @abstractmethod
    def split(self):
        # returns {group1: [view_files], group2: [view_files], ...}
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

        # just trying to extract the view file from the tuple
        splits = {label: [df_file_tuple[1] for df_file_tuple in views] for label, views in self.clusters.items()}
        return splits

    def representatives(self):
        # TODO: choose a random view in each group for now
        representatives = {}
        for label, views in self.clusters.items():
            representatives[label] = random.choice(views)
        return representatives


class PrimaryKey(DiscreteSignal):

    def __init__(self, primary_keys):
        # dict['pk'] = [views]
        self.primary_keys = primary_keys

    def evaluate(self):
        pass

    def split(self):
        splits = {pk: [df_file_tuple[1] for df_file_tuple in views] for pk, views in self.primary_keys.items()}
        return splits

    def representatives(self):
        # TODO: choose a random view in each group for now
        representatives = {}
        for pk, views in self.primary_keys.items():
            representatives[pk] = random.choice(views)
        return representatives


class ContradictoryViews(DiscreteSignal):

    def __init__(self, contradictions):
        # dict[contradictory row] = [(row_df, view_file)]
        self.contradictions = contradictions

    def evaluate(self):
        pass

    def split(self):
        splits = {row: [df_file_tuple[1] for df_file_tuple in views] for row, views in self.contradictions.items()}
        return splits

    def representatives(self):
        # TODO: show the two contradictory rows
        representatives = {}
        for row, views in self.contradictions.items():
            # although it's random, the contradictory row's dataframe is the same (except the index)
            representatives[row] = random.choice(views)
        return representatives


def pick_best_signal_to_split(splits):
    # If the user randomly pick a branch in the split, what's the expected value of uncertainty removed?
    # ev = sum over p_i * x_i
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
            max_gain = gain
            best_signal = signal
    return best_signal


if __name__ == '__main__':

    import glob
    import pandas as pd
    import pprint

    pd.set_option('display.max_columns', None)
    # pd.set_option('display.max_rows', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)  # or 199

    dir_path = "./test"

    # Run 4C
    results = v4c.main(dir_path)

    # TODO: don't separate by schemas for now
    compatible_groups = []
    contained_groups = []
    complementary_groups = []
    contradictory_groups = []
    for k, v in results[0].items():
        compatible_groups = compatible_groups + v['compatible']
        contained_groups = contained_groups + v['contained']
        complementary_groups = complementary_groups + v['complementary']
        contradictory_groups = contradictory_groups + v['contradictory']

    csv_files = glob.glob(dir_path + "/view_*")
    view_dfs = []
    has_compatible_view_been_added = [False] * len(compatible_groups)

    for f in csv_files:
        # remove duplicates in compatible groups
        already_added = False
        for i, compatible_group in enumerate(compatible_groups):
            if f in compatible_group:
                if has_compatible_view_been_added[i]:
                    already_added = True
                else:
                    has_compatible_view_been_added[i] = True

        if not already_added:
            df = pd.read_csv(f)
            view_dfs.append((df, f))

    print("\n--------------------------------------------------------------------------")
    print("Number of views: ", len(csv_files))
    print("After removing duplicates in compatible groups: ", len(view_dfs))

    signals = []
    view_files = [v[1] for v in view_dfs]

    # complementary views
    # print("Complementary views: ")
    complementary_views_count = 0
    for path1, path2, _, _, _ in complementary_groups:
        if path1 in view_files and path2 in view_files:
            complementary_views_count += 1
            # print(path1 + " - " + path2)
    print("Found ", complementary_views_count, " pair of complementary views")

    # Integrate contradictory groups as signals
    contradictions_dict = {}
    contradictions_dict_dedup = {}  # for deduplication
    for path1, composite_key_tuple, key_value_tuples, path2 in contradictory_groups:

        if not (path1 in view_files or path2 in view_files):
            continue

        # TODO: reading csv multiple times...
        df1 = pd.read_csv(path1)
        df1 = v4c.normalize(df1)
        df1 = df1.sort_index(axis=1)
        df2 = pd.read_csv(path2)
        df2 = v4c.normalize(df2)
        df2 = df2.sort_index(axis=1)

        composite_key = list(composite_key_tuple)
        key_values = list(key_value_tuples)

        for key_value in key_values:
            condition1 = (df1[composite_key[0]] == key_value[0])
            condition2 = (df2[composite_key[0]] == key_value[0])
            for i in range(1, len(composite_key)):
                condition1 = (condition1 & (df1[composite_key[i]] == key_value[i]))
                condition2 = (condition2 & (df2[composite_key[i]] == key_value[i]))

            row1_df = df1.loc[condition1]
            row2_df = df2.loc[condition2]

            # TODO: I want to use (row1, row2) as key for my dictionary but using to_string seems too hacky
            row1 = row1_df.to_string(header=False, index=False, index_names=False)
            row2 = row2_df.to_string(header=False, index=False, index_names=False)

            # (row1, row2) and (row2, row1) count as the same contradiction
            already_added = False
            if (row1, row2) in contradictions_dict_dedup.keys():
                already_added = True
            elif (row2, row1) in contradictions_dict_dedup.keys():
                row1, row2 = row2, row1
                path1, path2 = path2, path1
                row1_df, row2_df = row2_df, row1_df
            if already_added:
                # This is kind of ugly. But since I can't add the tuple directly to a set, I need to have a separate
                # dict for deduplication of paths
                if path1 not in contradictions_dict_dedup[(row1, row2)][0]:
                    contradictions_dict[(row1, row2)][0].append((row1_df, path1))
                if path2 not in contradictions_dict_dedup[(row1, row2)][1]:
                    contradictions_dict[(row1, row2)][1].append((row2_df, path2))

                contradictions_dict_dedup[(row1, row2)][0].add(path1)
                contradictions_dict_dedup[(row1, row2)][1].add(path2)
            else:
                contradictions_dict_dedup[(row1, row2)] = ({path1}, {path2})  # use set to avoid duplicates
                contradictions_dict[(row1, row2)] = ([(row1_df, path1)], [(row2_df, path2)])

    print("Found ", len(contradictions_dict), " contradictions\n")

    # One signal for each contradiction
    for contradiction, views_tuple in contradictions_dict.items():
        contradiction_dict = {}
        # Two groups of views, each corresponds to a contradictory row
        contradiction_dict[contradiction[0]] = views_tuple[0]
        contradiction_dict[contradiction[1]] = views_tuple[1]

        contradiction_signal = ContradictoryViews(contradiction_dict)
        signals.append(contradiction_signal)

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
    # signals.append(pk_signal)

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
    while num_iters < 100 and len(splits) > 0:

        # Pick the best split that can prune out most views (based on expected value)
        best_signal = pick_best_signal_to_split(splits)
        print("Best signal to split: ", best_signal.__class__.__name__)

        # Present a representative view (or sample) from each branch
        representatives = best_signal.representatives()

        # skipping user interaction part...
        print("Pick your preferred representative:")
        pprint.pprint(list(representatives.values()))
        print("{},\t{:>30}".format("Option", "Number of views in this group"))
        for option, views in splits[best_signal].items():
            print("{},\t{:>30}".format(option, len(views)))

        # user randomly picks one branch from the representatives
        random_pick = random.choice(list(representatives.keys()))
        print("User chooses option ", random_pick, "\n")

        # TODO: let user assign their own score to each representative instead of picking just one, and update
        #  ranking model accordingly

        # +1 to every view in the chosen group / cluster
        for view_file in splits[best_signal][random_pick]:
            ranking_model[view_file] += 1

        if isinstance(best_signal, ContinuousSignal):
            # Continue splitting on the cluster chosen
            # TODO: maybe user wants to re-split on multiple clusters? Keep all clusters that have scores above some
            #  confidence bounds? (might be overkill...)
            splits[best_signal] = best_signal.split(threshold=split_threshold, cluster=random_pick)
            # If there's only one cluster left, remove it
            if len(splits[best_signal]) <= 1:
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
