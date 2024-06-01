import copy
import numpy as np
import pandas as pd
from IPython.display import Markdown
from IPython.display import display_html
from view_presentation.interface.interface import interface
from view_presentation.interface import embedding_distance
import ipywidgets as widgets
from IPython.display import HTML
from IPython.display import clear_output

from view_distillation.vd import ViewDistillation

class CDatasetInterfaceAttributeSim(interface):
    def __init__(self,name,embedding_obj=None):
        self.name=name
        self.asked_questions={}
        self.curr_question_iter=0
        self.embedding_obj=embedding_obj

    def generate_candidates (self, vd: ViewDistillation):
        self.vd = vd
        self.candidates = dict()

        _contradictions = vd.contradictions

        # self.candidates format:
        # key: ((view1, view2), (key_attr1, key_attr2, ...))
        # value: {(key1_attr1, key1_attr2, ...), (key2_attr1, key2_attr2, ...), ...}
        for cont_view_pair, cont in _contradictions.items():
            for cont_attr_keys, cont_keys in cont.items():
                self.candidates[(cont_view_pair, cont_attr_keys)] = cont_keys

    def rank_candidates (self, query):
        # Score is based on the average semantic distance between every contradictory cells
        # Lower score (more semantically similar) pairs are ranked higher (prioritized)
        scores = dict()

        for k, v in self.candidates.items():
            v = list(v)
            if len(k[1]) == 1:
                v = [x[0] for x in v]

            df1 = self.vd.get_df(k[0][0]).set_index(list(k[1])).loc[v].reset_index()
            df2 = self.vd.get_df(k[0][1]).set_index(list(k[1])).loc[v].reset_index()

            ne_pos = (df1.ne(df2)).stack()
            ne_pos = ne_pos[ne_pos]

            curr_score = 0.0
            curr_count = 0.0
            for row, col in ne_pos.index:
                dist = self.embedding_obj.get_distance(df1.at[row, col], df2.at[row, col])
                curr_count += 1.0
                curr_score = curr_score + (dist - curr_score) / curr_count

            scores[k] = curr_score

        self.sorted_sc = sorted(scores.items(), key=lambda item: item[1],reverse=False)
        return self.sorted_sc

    def get_question(self, ignored_datasets=[], ignore_questions=[]):
        iter = self.curr_question_iter

        while iter < len(self.sorted_sc):
            # Check if question is already asked or dataset in question is ignored
            if self.sorted_sc[iter][0] in ignore_questions or self.sorted_sc[iter][0][0][0] in ignored_datasets or self.sorted_sc[iter][0][0][1] in ignored_datasets:
                iter += 1
                continue
            break

        self.curr_question_iter = iter
        if iter >= len(self.sorted_sc):
            return None

        # TODO: Fix coverage return
        return (1, self.sorted_sc[iter][0], [self.sorted_sc[iter][0]])
        
    def highlight_diff(self,df1, df2, color='pink'):
        # Define html attribute
        attr = 'background-color: {}'.format(color)
        # Where df1 != df2 set attribute
        return pd.DataFrame(np.where(df1.ne(df2), attr, ''), index=df1.index, columns=df1.columns)


    def highlight_cols(self,s, color='lightgreen'):
        return 'background-color: %s' % color

    def ask_question_gui(self, question, df_lst):
        cont_pair = question[0]
        key_attrs = question[1]
        key_rows = list(self.candidates[question])[:5]
        key_attrs = list(key_attrs)

        if len(key_attrs) == 1:
            key_rows = [x[0] for x in key_rows]

        contradictory_rows1 = self.vd.get_df(cont_pair[0]).set_index(key_attrs).loc[key_rows].reset_index()
        contradictory_rows2 = self.vd.get_df(cont_pair[1]).set_index(key_attrs).loc[key_rows].reset_index()

        display(Markdown('<h3><strong>{}</strong></h3>'.format(f"Below are contradicting datasets (with key = {key_attrs}), which dataset would you shortlist?")))#This Would you shortlist this dataset for the query? ")))#Do you want to shortlist datasets containing the attribute: "+question)))
        html1 = contradictory_rows1.style \
                            .applymap(self.highlight_cols, subset=pd.IndexSlice[:, key_attrs],
                                      color='lightyellow') \
                            .apply(self.highlight_diff, axis=None, df2=contradictory_rows2) \
                            .to_html()

        html2 = contradictory_rows2.style \
                            .applymap(self.highlight_cols, subset=pd.IndexSlice[:, key_attrs],
                                      color='lightyellow') \
                            .apply(self.highlight_diff, axis=None, df2=contradictory_rows1) \
                            .to_html()
        
        display(Markdown('<h3><strong>{}</strong></h3>'.format("A:")))
        display(HTML(html1)) 
        display(Markdown('<h3><strong>{}</strong></h3>'.format("B:")))
        display(HTML(html2))
        self.curr_question_iter += 1
        #display(df_lst[question].head(10))

        self.attribute_yesno=widgets.RadioButtons(
            options=['A: Choose the first one', 'B: Choose the second one','None'],
            value='None', # Defaults to 'pineapple'
            description='',
            disabled=False
        )

        self.submit = widgets.Button(
            description='Submit',
            disabled=False,
            button_style='', # 'success', 'info', 'warning', 'danger' or ''
            tooltip='Submit',
            icon='' # (FontAwesome names without the `fa-` prefix)
        )

        self.submit.on_click(self.returnval)
        display(self.attribute_yesno)
        display(self.submit)

        return [['Yes, my data must contain this dataset', 'No, my data should not contain this dataset','Does not matter'],self.attribute_yesno,self.submit]

    def returnval(self,b):
        return self.attribute_yesno.value
    
    def ask_question(self, question, df_lst):
        return self.ask_question_gui(question, df_lst)
    
    #TODO: Change the value of dictionary to the answer from the user
    #def ask_question(self, question, df_lst):
        self.curr_question_iter += 1
        '''
        print ("Does the required dataset contain this dataframe: ",df_lst[question])
        print ("Enter your option:")
        print ("1: Always present")
        print ("2: Never present")
        print ("3: May be present")
        while True:
            option=int(input("Enter your option "))
            try:
                if option in [1,2,3]:
                    self.asked_questions[question] = option
                    break
            except:
                print ("invalid option")

        return option

        self.asked_questions[question] = 1
        print ("Asking a dataset question")
        '''


#example usage of the interface
# if __name__ == '__main__':

#     data1 = ["Chicago","NYC","SF","Seattle"]
#     df1 = pd.DataFrame(data1, columns=['City'])
  
#     data2 = ["Paris","Copenhagen","Delhi","Sydney"]
#     df2 = pd.DataFrame(data2, columns=['international city'])

#     df_lst=[df1,df2]

   
#     print ("Dataset Interface")
#     embedding_obj = embedding_distance.EmbeddingModel()
#     attr_inf=DatasetInterfaceAttributeSim("header content interface",embedding_obj)
#     attr_inf.generate_candidates(df_lst)
#     print(attr_inf.attr_dic)

    
#     print(attr_inf.rank_candidates("new york city"))


#     print(attr_inf.get_question())

    
