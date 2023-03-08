## View Presentation
This component interacts with the user to help prune useless datasets and choose the desired ones. Internally, it employs a bandit-based algorithm to explore the options.
Inputs:
a) Input query
b) List of datasets

Output: Set of datasets chosen by the user

The component interacts with the user through a suite of interfaces defined in interface/. Please refer to interface/README.md to add new interface options.

Usage:

1. Specify the set of interfaces in interface_list.py
2. Import ViewPresentation class from ver_view_presentation.py
3. Define an object of class ViewPresentation as follows

```
vp = ViewPresentation([query], [df_lst]) 
```

df_lst denotes the list of dataframes
query denotes the input query by the user
   
4. Run choose_interface. This function runs bandit based algorithm to choose an interface and corresponding question. It poses a question to the user whose response is used to re-rank the set of candidate datasets.

```
 vp.choose_interface()
```

5. Run step 4 untill a useful dataset has been found.

6. Check ranking of datasets

```
 vp.get_shortlisted_datasets()
```
It returns a ranklist of views in non-increasing order of preference, computed based on user's responses.
