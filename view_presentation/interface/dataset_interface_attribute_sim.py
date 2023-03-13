import pandas as pd
from view_presentation.interface.interface import interface
import view_presentation.interface.embedding_distance as embedding_distance
import ipywidgets as widgets

from IPython.display import clear_output


class DatasetInterfaceAttributeSim(interface):
    def __init__(self,name,embedding_obj=None):
        self.name=name
        self.asked_questions={}
        self.curr_question_iter=0
        self.embedding_obj=embedding_obj

    def generate_candidates (self, df_lst):
        self.attr_dic={}
        iter=0
        while iter<len(df_lst):
            df=df_lst[iter]
            try:
                attr_lst= list(df.columns)
                self.attr_dic[iter] = ' '.join(attr_lst)
                iter+=1
            except:
                iter+=1
                continue

    def rank_candidates (self, query): 
        self.scores={}
        for df_iter in self.attr_dic.keys():
            dist=self.embedding_obj.get_distance(self.attr_dic[df_iter],query)#model.wmdistance(attr.split(),query.split())
            self.scores[df_iter]=dist
        self.sorted_sc=sorted(self.scores.items(), key=lambda item: item[1],reverse=False)
        return self.sorted_sc

    #Returns the data frame with the highest score
    def get_question(self, ignored_datasets=[],ignore_questions=[]):
        iter=self.curr_question_iter
        while iter<len(self.sorted_sc):
            if self.sorted_sc[iter][0] in ignore_questions or self.sorted_sc[iter][0] in ignored_datasets:
                iter+=1
                continue
            else:
                break
        if iter <len(self.sorted_sc):
            curr_question =self.sorted_sc[iter][0]
            self.curr_question_iter = iter
            #returns the location of chosen df 
            return (1, curr_question,[curr_question])
        else:
            return None

    def ask_question_gui(self, question, df_lst):
        self.curr_question_iter += 1
        print ("Would you shortlist this dataset for the query? ")
        display(df_lst[question])

        self.attribute_yesno=widgets.RadioButtons(
            options=['Yes, my data must contain this dataset', 'No, my data should not contain this dataset','Does not matter'],
            value='Does not matter', # Defaults to 'pineapple'
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
if __name__ == '__main__':

    data1 = ["Chicago","NYC","SF","Seattle"]
    df1 = pd.DataFrame(data1, columns=['City'])
  
    data2 = ["Paris","Copenhagen","Delhi","Sydney"]
    df2 = pd.DataFrame(data2, columns=['international city'])

    df_lst=[df1,df2]

   
    print ("Dataset Interface")
    embedding_obj = embedding_distance.EmbeddingModel()
    attr_inf=DatasetInterfaceAttributeSim("header content interface",embedding_obj)
    attr_inf.generate_candidates(df_lst)
    print(attr_inf.attr_dic)

    
    print(attr_inf.rank_candidates("new york city"))


    print(attr_inf.get_question())

    