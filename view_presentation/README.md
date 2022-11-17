## View Presentation
This component interacts with the user to help prune useless datasets and choose the desired ones. Internally, it employs a bandit-based algorithm to explore the options.
Inputs:
a) Input query
b) List of datasets

The component interacts with the user through a suite of interfaces defined in interface/. Please refer to interface/README.md to add new interface options.

Usage:

1. Specify the set of interfaces in interface_list.py
2. Define an object of class ViewPresentation as follows

vp = ViewPresentation([query], [df_lst]) 

df_lst denotes the list of dataframes
query denotes the input query by the user
   
3. Run choose_interface. This function runs bandit based algorithm to choose an interface and corresponding question. It poses a question to the user whose response is used to re-rank the set of candidate datasets.


 vp.choose_interface()

4. Run step 3 untill a useful dataset has been found.

