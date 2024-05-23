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
        self.cont_dic = vd.contradictions
        # self.attr_dic={}
        # iter=0
        # while iter<len(df_lst):
        #     df=df_lst[iter]
        #     try:
        #         attr_lst= list(df.columns)
        #         self.attr_dic[iter] = ' '.join(attr_lst)
        #         iter+=1
        #     except:
        #         iter+=1
        #         continue

    def rank_candidates (self, query):
        # TODO: Implement
        _scores = {}
        i = 1.0

        for cont in self.cont_dic.keys():
            _scores[cont] = i
            i += 0.1

        # self.scores={}
        # for df_iter in self.attr_dic.keys():
        #     dist=self.embedding_obj.get_distance(self.attr_dic[df_iter],query)#model.wmdistance(attr.split(),query.split())
        #     self.scores[df_iter]=dist

        self.sorted_sc = sorted(_scores.items(), key=lambda item: item[1],reverse=False)
        return self.sorted_sc

    #Returns the data frame with the highest score
    def get_question(self, ignored_datasets=[], ignore_questions=[]):
        return (self.sorted_sc[0][1], self.sorted_sc[0][0], [self.sorted_sc[0][0]])
        # iter=self.curr_question_iter
        # while iter<len(self.sorted_sc):
        #     if self.sorted_sc[iter][0] in ignore_questions or self.sorted_sc[iter][0] in ignored_datasets:
        #         iter+=1
        #         continue
        #     else:
        #         break
        # if iter <len(self.sorted_sc):
        #     curr_question =self.sorted_sc[iter][0]
        #     self.curr_question_iter = iter
        #     #returns the location of chosen df 
        #     return (1, curr_question,[curr_question])
        # else:
        #     return None
        
    def highlight_diff(self,df1, df2, color='pink'):
        # Define html attribute
        attr = 'background-color: {}'.format(color)
        # Where df1 != df2 set attribute
        return pd.DataFrame(np.where(df1.ne(df2), attr, ''), index=df1.index, columns=df1.columns)


    def highlight_cols(self,s, color='lightgreen'):
        return 'background-color: %s' % color

    def ask_question_gui(self, question, df_lst):
        key_tuple = list(self.cont_dic[question].keys())[0]
        key_rows = [x[0] for x in self.cont_dic[question][key_tuple]][:5]

        contradictory_rows1 = self.vd.get_df(question[0]).set_index(key_tuple[0]).loc[key_rows].reset_index()
        contradictory_rows2 = self.vd.get_df(question[1]).set_index(key_tuple[0]).loc[key_rows].reset_index()

        display(Markdown('<h3><strong>{}</strong></h3>'.format(f"Below are contradicting datasets (with key = {key_tuple[0]}), which dataset would you shortlist?")))#This Would you shortlist this dataset for the query? ")))#Do you want to shortlist datasets containing the attribute: "+question)))
        html1 = contradictory_rows1.style \
                            .applymap(self.highlight_cols, subset=pd.IndexSlice[:, list(key_tuple)],
                                      color='lightyellow') \
                            .apply(self.highlight_diff, axis=None, df2=contradictory_rows2) \
                            .to_html()

        html2 = contradictory_rows2.style \
                            .applymap(self.highlight_cols, subset=pd.IndexSlice[:, list(key_tuple)],
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

    
