import pandas as pd


import ipywidgets as widgets
from IPython.display import clear_output
#Add properties of a dataframe here whichever we want to use for indexing
class Ver_DataFrame(pd.DataFrame):
    def __init__(self, *args, **kw):
        global fout
        global candidate_lst,candidate_attributes,attribute_to_dataset,candidate_name_to_id
        global shortlisted_lst,pruned_lst
        super(Ver_DataFrame, self).__init__(*args, **kw)
        self._name='test'
        self.shortlistval=0
        
        self.throwbutton = widgets.Button(
                description='Remove it',
                disabled=False,
                button_style='', # 'success', 'info', 'warning', 'danger' or ''
                tooltip='Submit',
                icon='' # (FontAwesome names without the `fa-` prefix)
            )
        self.shortlistbutton = widgets.Button(
                description='Shortlist it',
                disabled=False,
                button_style='', # 'success', 'info', 'warning', 'danger' or ''
                tooltip='Submit',
                icon='' # (FontAwesome names without the `fa-` prefix)
            )
        self.selectbutton = widgets.Button(
                description='Select it',
                disabled=False,
                button_style='', # 'success', 'info', 'warning', 'danger' or ''
                tooltip='Submit',
                icon='' # (FontAwesome names without the `fa-` prefix)
            )
        self.throwbutton.on_click(self.on_throw_clicked)
        self.selectbutton.on_click(self.on_selection)
        self.shortlistbutton.on_click(self.on_shortlist_clicked)
     
    def on_throw_clicked(self,b):
        global shortlisted_lst,pruned_lst
        self.shortlistval=-1
        start=timeit.default_timer()
        fout.write("remove dataset clicked:"+str(start)+"\n")
        print (candidate_name_to_id,self._name,shortlisted_lst)
        if candidate_name_to_id[self._name] in shortlisted_lst:
            shortlisted_lst.remove(candidate_name_to_id[self._name])
        pruned_lst.append(candidate_name_to_id[self._name])
        pruned_lst=list(set(pruned_lst))
    def on_shortlist_clicked(self,b):
        global shortlisted_lst,pruned_lst
        shortlisted_lst.append(candidate_name_to_id[self._name])
        if candidate_name_to_id[self._name] in pruned_lst:
            pruned_lst.remove(candidate_name_to_id[self._name])
        shortlisted_lst=list(set(shortlisted_lst))
        self.shortlistval=1
        
    def on_selection(self,b):
        clear_output()
        print ("You have completed the task")
        fout.write("Dataset has been selected\n")
        print (self.to_string())
        fout.write(self.to_string()+"\n")
        start = timeit.default_timer()
        fout.write("Ending the task: "+str(start)+"\n")
        fout.close()
    def set_name(self,name):
        self._name=name

