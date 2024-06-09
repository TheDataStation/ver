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
    def __init__(self, name, shortlist_func, ignore_func, embedding_obj=None):
        self.name=name
        self.asked_questions={}
        self.curr_question_iter=0
        self.embedding_obj=embedding_obj

        self.shortlist_func = shortlist_func
        self.ignore_func = ignore_func

        self.OPTIONS = ['A: Choose the first one', 'B: Choose the second one', 'None']

    def generate_candidates (self, vd: ViewDistillation, mapping):
        self.vd = vd
        self.candidates = dict()
        self.mapping = mapping

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
            if self.sorted_sc[iter][0] in ignore_questions \
                    or self.mapping[self.sorted_sc[iter][0][0][0]] in ignored_datasets \
                    or self.mapping[self.sorted_sc[iter][0][0][1]] in ignored_datasets:
                iter += 1
                continue
            break

        self.curr_question_iter = iter
        if iter >= len(self.sorted_sc):
            return None

        return (1, self.sorted_sc[iter][0])

    def highlight_cells(self, df1, df2, key_columns):
        def highlight(row):
            return [
                'background-color: lightyellow' if col in key_columns else 
                'background-color: pink' if row[col] != df2.at[row.name, col] else 
                ''
                for col in df1.columns
            ]
        
        # Apply the highlight function to the entire DataFrame
        return df1.style.apply(highlight, axis=1)

    def ask_question_gui(self, question, df_lst):
        self.curr_question = question
        self.curr_question_iter += 1

        cont_pair = question[0]
        key_attrs = question[1]
        key_rows = list(self.candidates[question])[:5]
        key_attrs = list(key_attrs)

        if len(key_attrs) == 1:
            key_rows = [x[0] for x in key_rows]

        contradictory_rows1 = self.vd.get_df(cont_pair[0]).set_index(key_attrs).loc[key_rows].reset_index()
        contradictory_rows2 = self.vd.get_df(cont_pair[1]).set_index(key_attrs).loc[key_rows].reset_index()
        keys_display = 'with key' + ('s' if len(key_attrs) > 1 else '') + ' = ' + ' '.join(key_attrs)

        display(Markdown('<h3><strong>{}</strong></h3>'.format(f"Below are contradicting datasets ({keys_display}), which dataset would you shortlist?")))
        html1 = self.highlight_cells(contradictory_rows1, contradictory_rows2, key_attrs)
        html2 = self.highlight_cells(contradictory_rows2, contradictory_rows1, key_attrs)
        
        display(Markdown('<h3><strong>{}</strong></h3>'.format("A:")))
        display(html1)
        display(Markdown('<h3><strong>{}</strong></h3>'.format("B:")))
        display(html2)

        self.attribute_yesno=widgets.RadioButtons(
            options=self.OPTIONS,
            value=self.OPTIONS[-1],
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

        self.submit.on_click(self.update_score)

        display(self.attribute_yesno)
        display(self.submit)
        return

    def update_score(self, b):
        answer = self.OPTIONS.index(self.attribute_yesno.value)
        a = self.mapping[self.curr_question[0][0]]
        b = self.mapping[self.curr_question[0][1]]

        if answer == 0:
            self.shortlist_func([a])
            self.ignore_func([b])
        elif answer == 1:
            self.shortlist_func([b])
            self.ignore_func([a])

        self.submit_callback()
        return
    
    def ask_question(self, question, df_lst, submit_callback):
        self.submit_callback = submit_callback
        self.ask_question_gui(question, df_lst)
        return
    
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

    
