# View Distillation

## Input

`path_to_views`: path to the directory containing views in csv format 
  
## Create an instance

```python
from view_distillation import ViewDistillation

vd = ViewDistillation(path_to_views)
```

## High-level APIs

### Get current views

```python
vd.get_current_views()
```

This will return the list of current views. 
Views that were pruned from any previous API calls will be excluded.

### Distill views

```python
vd.distill_views(remove_identical_views=True, 
                 remove_contained_views=True,
                 union_complementary_views=True)
```

If `remove_identical_views` is `True`, it will find all views that are identical with
each other, keep one of the view and remove the rest.

If `remove_contained_views` is `True`, it will find all views that are contained in some larger view
and remove them.

If `union_complementary_views` is `True`, it will find all pair of views 
that are complementary with each other without any contradictions, 
union each pair of views and materialize the new view in the input directory.
The complementary views used to union are removed.

This API will return the list of views left after distillation.

### Interactive tool for choosing among contradictory views

Works in Jupyter notebook with Jupyter Widgets installed.

```jupyterpython
%gui asyncio

task = vd.present_contradictory_views_demo()

await task

views_pruned, views_selected, views_left = task.result()
```

After you exit, it will return the views you have pruned, selected, 
and current views left.

## Low-level APIs

### Find compatible views

```python
vd.find_compatible_views()
```

This will return a list of lists, each inner list contains the views that are identical to each other.

### Prune compatible views

```python
vd.reduce_compatible_views_to_one()
```

This will prune all compatible views and leave one, any future API calls will operate on the rest of views.
It will also return the list of views left after pruning.

### Find contained views

```python
vd.find_contained_views()
```

This will return a list of lists, each inner list contains views in sorted order (descending by view size) 
where each view contains all the views that are smaller.

### Prune contained views

```python
vd.prune_contained_views(keep_largest=True)
```

If the parameter `keep_largest` is True, it will prune all contained views and only keep the largest view in each group, 
any future API calls will operate on the rest of views. It will also return the list of views left after pruning.

(note: we may want to allow different pruning strategies that users can set)

### Find contradictory views

```python
vd.find_contradictory_views()
```

This will return a `dict`. The key is a tuple `(view1, view2, key)` and the corresponding value
is a `set` of key values where `view1` and `view2` contradict on. The `key` is a `tuple` since it 
can be a composite key containing more than one attributes, and each key value is also a `tuple`.

For example, if the `key` is the `id` column in `view1` and `view2`, and the key value is `123`, this means
`view1` and `view2` both contains the row with `id = 123` but the rest of values in the row differ.

### Find complementary views

```python
vd.find_complementary_views()
```

This will return a list of tuples. Each tuple `(view1, view2, key)` means `view1` and `view2` are complementary on `key`,
in other words, `view1` and `view2` don't have any contradictions on `key`.

### Union complementary views

```python
vd.union_complementary_views()
```

This will find all pair of views that are complementary with each other without any contradictions, 
union each pair of views and materialize the new view in the input directory.
The complementary views used to union are removed.


## Helper Functions

```python
vd.get_row_from_key(df, key, key_value)
```
 
The input are `df`, a pandas Dataframe where the row will be extracted,
`key`, the key column name, it should be a `tuple` since 
the key can be composite and contain multiple columns,
and `key_value`, the corresponding key value, also a `tuple`.

This will return the row as a Dataframe based on the provided `key` and `key_value` in `df`.






