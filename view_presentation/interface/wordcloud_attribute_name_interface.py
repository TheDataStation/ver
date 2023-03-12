from view_presentation.interface.interface import interface
import view_presentation.interface.embedding_distance as embedding_distance
import pandas as pd
import view_presentation.interface.cluster as cluster
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import ipywidgets as widgets

class WordCloudAttributeNameInterface(interface):
    def __init__(self,name,embedding_obj=None,k=100):
        self.name=name
        self.asked_questions={}
        self.curr_question_iter=0
        self.embedding_obj=embedding_obj
        self.k=k

    def generate_candidates (self, df_lst):

        self.attr_dic={}
        self.attributes=[]
        self.attr_to_val={}
        iter=0
        #print (len(df_lst))
        while iter<len(df_lst):
            df=df_lst[iter]
            
            try:
                attr_lst= list(df.columns)
                #self.attributes.extend(attr_lst)
                for attr in list(df.columns):
                    if len(attr)<3:
                        continue
                    attr_df_lst=[]
                    val_lst=[]
                    if attr in self.attr_dic.keys():
                        attr_df_lst=self.attr_dic[attr]
                        val_lst=self.attr_to_val[attr]
                    attr_df_lst.append(iter)
                    val_lst.extend(list(df[attr]))
                    self.attr_to_val[attr]=val_lst
                    self.attr_dic[attr]=attr_df_lst
                iter+=1
            except:
                iter+=1
                continue

        self.attributes = list(self.attr_dic.keys())
        #print (self.attributes,self.attr_to_val)
        self.centers_attr,self.center_attr,pred1,max_dist,pred_map_attr=cluster.cluster(self.attributes,self.attr_to_val,[],0,self.embedding_obj,self.k)
        self.cluster_attr={}
        for attr in pred_map_attr.keys():
            l=[]
            if pred_map_attr[attr] in self.cluster_attr.keys():
                l=self.cluster_attr[pred_map_attr[attr]]
            l.append(attr)
            self.cluster_attr[pred_map_attr[attr]]=l



    def rank_candidates (self, query): 
        self.scores={}
        for attr in self.center_attr:
            #print (attr,self.center_attr)
            dist=self.embedding_obj.get_distance(attr,query)#model.wmdistance(attr.split(),query.split())
            self.scores[attr]=dist
        self.sorted_sc=sorted(self.scores.items(), key=lambda item: item[1],reverse=False)
        return self.sorted_sc

            
    #Returns the attribute with highest score, ignoring the attributes in ignore_questions
    def get_question(self, ignored_datasets=[],ignore_questions=[]):
        iter=self.curr_question_iter
        #print ("sorted",self.sorted_sc,iter)
        while iter<len(self.sorted_sc):
            if self.sorted_sc[iter][0] in ignore_questions:
                iter+=1
                continue
            else:
                break
        curr_question = self.sorted_sc[iter][0]
        
               
        self.curr_question_iter = iter
        #print ("sorted2",self.sorted_sc,iter)
        #returns the chosen attribute and list of dataframes containing the attribute

        lst = self.cluster_attr[curr_question] 

        coverage = list(set(self.attr_dic[curr_question]) - set(ignored_datasets))

        return (len(coverage), lst, self.attr_dic[curr_question])

    def ask_question_gui(self, question, df_lst):
        self.curr_question_iter += 1
        print ("Does the required dataset contain attribute: ",question)
        self.attribute_yesno=widgets.RadioButtons(
            options=['Yes, my data must contain this attribute', 'No, the data should not contain this attribute','Does not matter'],
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
        self.curr_question=question
        self.submit.on_click(self.returnval)
        display(self.attribute_yesno)
        display(self.submit)

        return [['Yes, my data must contain this attribute', 'No, my data should not contain this attribute','Does not matter'],self.attribute_yesno,self.submit]

    def returnval(self,b):
        for ques in self.curr_question:
            self.asked_questions[ques] = self.attribute_yesno.value
        #self.asked_questions[question] = self.attribute_yesno.value
        return self.attribute_yesno.value
    def ask_question(self, question, df_lst):
        return self.ask_question_gui(question, df_lst)
        '''
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
        '''


#example usage of the interface
if __name__ == '__main__':

    data1 = ["Chicago","NYC","SF","Seattle"]
    df1 = pd.DataFrame(data1, columns=['City'])
  
    data2 = ["Paris","Copenhagen","Delhi","Sydney"]
    df2 = pd.DataFrame(data2, columns=['international city'])

    df_lst=[df1,df2]
    embedding_obj = embedding_distance.EmbeddingModel()
   
    print ("WordCloud Interface")
    attr_inf=WordCloudAttributeNameInterface("wordcloud interface",embedding_obj,1)
    attr_inf.generate_candidates(df_lst)

    print(attr_inf.attr_dic)

    
    print(attr_inf.rank_candidates("new york city"))

    print(attr_inf.get_question())

    