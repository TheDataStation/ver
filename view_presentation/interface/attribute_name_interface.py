from view_presentation.interface.interface import interface
import pandas as pd
import view_presentation.interface.embedding_distance as embedding_distance

class AttributeNameInterface(interface):
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
                for attr in list(df.columns):
                    if len(attr)<3:
                        continue
                    attr_df_lst=[]
                    if attr in self.attr_dic.keys():
                        attr_df_lst=self.attr_dic[attr]
                    attr_df_lst.append(iter)
                    self.attr_dic[attr]=attr_df_lst
                iter+=1
            except:
                iter+=1
                continue

    def rank_candidates (self, query, embedding_obj): 
        self.scores={}
        for attr in self.attr_dic.keys():
            dist=embedding_obj.get_distance(attr,query)#model.wmdistance(attr.split(),query.split())
            self.scores[attr]=dist
        self.sorted_sc=sorted(self.scores.items(), key=lambda item: item[1],reverse=False)
        return self.sorted_sc

            
    #Returns the attribute with highest score, ignoring the attributes in ignore_questions
    def get_question(self, ignored_datasets,ignore_questions=[]):
        iter=self.curr_question_iter
        while iter<len(self.sorted_sc):
            if self.sorted_sc[iter][0] in ignore_questions:
                iter+=1
                continue
            else:
                break
        curr_question = self.sorted_sc[iter][0]
        self.curr_question_iter = iter
        #returns the chosen attribute and list of dataframes containing the attribute
        coverage = list(set(self.attr_dic[curr_question]) - set(ignored_datasets))

        return (len(coverage), curr_question,self.attr_dic[curr_question])

    
    def ask_question(self, question, df_lst):
        self.curr_question_iter += 1
        print ("Does the required dataset contain attribute: ",question)
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


if __name__ == '__main__':

    data1 = ["Chicago","NYC","SF","Seattle"]
    df1 = pd.DataFrame(data1, columns=['City'])
  
    data2 = ["Paris","Copenhagen","Delhi","Sydney"]
    df2 = pd.DataFrame(data2, columns=['international city'])

    df_lst=[df1,df2]

    attr_inf=AttributeNameInterface("header interface")
    attr_inf.generate_candidates(df_lst)
    print(attr_inf.attr_dic)
    embedding_obj = embedding_distance.EmbeddingModel()
    print(attr_inf.rank_candidates("new york city",embedding_obj))
    print(attr_inf.get_question())

