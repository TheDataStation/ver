# View Distillation Design Doc

## Input

- A set of views
  - For now, only allow views to be in csv format
  - Can expand to different formats (ex: from databases) in the future

## Create an instance

`vd = ViewDistillation(path_to_views)`

## APIs

(The views are returned as paths.)

### Find compatible views

`result = vd.find_compatible_views()`

This will return a list of lists, each inner list contains the views that are identical to each other.

### Prune compatible views

`result = vd.reduce_compatible_views_to_one()`

This will prune all compatible views and leave one, any future API calls will operate on the rest of views.
It will also return the list of views left after pruning.

### Find contained views

`result = vd.find_contained_views()`

This will return a list of lists, each inner list contains views in sorted order (descending by view size) 
where each view contains all the views that are smaller.

### Prune contained views

`result = vd.prune_contained_views(keep_largest=True)`

If the parameter `keep_largest` is True, it will prune all contained views and only keep the largest view in each group, 
any future API calls will operate on the rest of views. It will also return the list of views left after pruning.

(note: we may want to allow different pruning strategies that users can set)

### Find contradictory views

`result = vd.find_contradictory_views()`

This will return a `dict`. The key is a tuple `(view1, view2, key)` and the corresponding value
is a `set` of key values where `view1` and `view2` contradict on. The `key` is a `tuple` since it 
can be a composite key containing more than one attributes, and each key value is also a `tuple`.

For example, if the `key` is the `id` column in `view1` and `view2`, and the key value is `123`, this means
`view1` and `view2` both contains the row with `id = 123` but the rest of values in the row differ.

### Find complementary views

`result = vd.find_complementary_views()`

This will return a list of tuples. Each tuple `(view1, view2, key)` means `view1` and `view2` are complementary on `key`,
in other words, `view1` and `view2` don't have any contradictions on `key`.







