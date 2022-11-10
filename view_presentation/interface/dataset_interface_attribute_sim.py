import pandas as pd
from interface import interface
import embedding_distance

class DatasetInterfaceAttributeSim(interface):
    def __init__(self,name):
        self.name=name
        self.asked_questions={}
        self.curr_question_iter=0

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

    def rank_candidates (self, query, embedding_obj): 
        self.scores={}
        for df_iter in self.attr_dic.keys():
            dist=embedding_obj.get_distance(self.attr_dic[df_iter],query)#model.wmdistance(attr.split(),query.split())
            self.scores[df_iter]=dist
        self.sorted_sc=sorted(self.scores.items(), key=lambda item: item[1],reverse=False)
        return self.sorted_sc

    #Returns the data frame with the highest score
    def get_question(self, ignore_questions=[]):
        iter=self.curr_question_iter
        while iter<len(self.sorted_sc):
            if self.sorted_sc[iter][0] in ignore_questions:
                iter+=1
                continue
            else:
                break
        curr_question =self.sorted_sc[iter][0]
        self.curr_question_iter = iter
        #returns the location of chosen df 
        return (iter, curr_question)

    #TODO: Change the value of dictionary to the answer from the user
    def ask_question(self, iter, question):
        self.curr_question_iter=iter+1
        self.asked_questions[question] = 1


#example usage of the interface
if __name__ == '__main__':

    data1 = ["Chicago","NYC","SF","Seattle"]
    df1 = pd.DataFrame(data1, columns=['City'])
  
    data2 = ["Paris","Copenhagen","Delhi","Sydney"]
    df2 = pd.DataFrame(data2, columns=['international city'])

    df_lst=[df1,df2]

   
    print ("Dataset Interface")
    attr_inf=DatasetInterfaceAttributeSim("header content interface")
    attr_inf.generate_candidates(df_lst)
    print(attr_inf.attr_dic)

    embedding_obj = embedding_distance.EmbeddingModel()
    print(attr_inf.rank_candidates("new york city",embedding_obj))


    print(attr_inf.get_question())

    