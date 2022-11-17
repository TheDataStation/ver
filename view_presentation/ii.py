import pandas as pd
import embedding_distance
class interface:
    def __init__(self,name):
        self.name=name
        self.asked_questions={}
        
    #def get_question()
        

    # ranking of questions

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
        #returns the chosen attribute and list of dataframes containing the attribute
        return (iter, curr_question,self.attr_dic[curr_question])

    #TODO: Change the value of dictionary to the answer from the user
    def ask_question(self, iter, question):
        self.curr_question_iter=iter+1
        self.asked_questions[question] = 1


#Ranks attributes based on their content
class AttributeContentInterface(AttributeNameInterface):

    def get_content(self, attr_val_lst):
        content_lst=[]
        for val in attr_val_lst:
            content_lst.append(val)    
        return ' '.join(content_lst)
    def generate_candidates (self, df_lst):
        self.attr_content_dic={}
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
                    attr_content=''
                    if attr in self.attr_dic.keys():
                        attr_df_lst=self.attr_dic[attr]
                        attr_content=self.attr_content_dic[attr]
                    attr_df_lst.append(iter)
                    attr_content += " "+(self.get_content(df[attr]))
                    self.attr_dic[attr]=attr_df_lst
                    self.attr_content_dic[attr] = attr_content
                iter+=1
            except:
                iter+=1
                continue

    def rank_candidates (self, query, embedding_obj): 
        self.scores={}
        for attr in self.attr_dic.keys():
            dist=embedding_obj.get_distance(self.attr_content_dic[attr],query)#model.wmdistance(attr.split(),query.split())
            self.scores[attr]=dist
        self.sorted_sc=sorted(self.scores.items(), key=lambda item: item[1],reverse=False)
        return self.sorted_sc

      


#example usage of the interface
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


    print ("Content interface")
    attr_inf=AttributeContentInterface("header content interface")
    attr_inf.generate_candidates(df_lst)
    print(attr_inf.attr_dic)

    embedding_obj = embedding_distance.EmbeddingModel()
    print(attr_inf.rank_candidates("USA cities",embedding_obj))


    print(attr_inf.get_question())


    print ("Dataset Interface")
    attr_inf=DatasetInterfaceAttributeSim("header content interface")
    attr_inf.generate_candidates(df_lst)
    print(attr_inf.attr_dic)

    embedding_obj = embedding_distance.EmbeddingModel()
    print(attr_inf.rank_candidates("new york city",embedding_obj))


    print(attr_inf.get_question())

    