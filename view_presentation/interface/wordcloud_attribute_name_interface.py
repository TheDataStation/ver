from view_presentation.interface.interface import interface
from view_presentation.interface import embedding_distance
import pandas as pd
import random
from IPython.display import Markdown
from IPython.display import display

# import view_presentation.interface.cluster as cluster
from view_presentation.interface import cluster
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import ipywidgets as widgets
import matplotlib.pyplot as plt

class WordCloudAttributeNameInterface(interface):
    def __init__(self, name, shortlist_func, ignore_func, embedding_obj=None, k=10):
        self.name=name
        self.asked_questions={}
        self.curr_question_iter=0
        self.embedding_obj=embedding_obj
        self.k=k

        self.shortlist_func = shortlist_func
        self.ignore_func = ignore_func

        self.OPTIONS = ['Yes, my data must contain this attribute', 'No, my data should not contain this attribute','Does not matter']

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
        # print (self.cluster_attr,self.center_attr)


    def rank_candidates (self, query): 
        self.scores={}
        for attr in self.center_attr:
            #print (attr,self.center_attr)
            dist=self.embedding_obj.get_distance(attr,query)#model.wmdistance(attr.split(),query.split())
            self.scores[attr]=dist
        self.sorted_sc=sorted(self.scores.items(), key=lambda item: item[1],reverse=False)
        return self.sorted_sc

            
    #Returns the attribute with highest score, ignoring the attributes in ignore_questions
    def get_question(self, ignored_datasets=[], ignore_questions=[]):
        iter = self.curr_question_iter
        while iter<len(self.sorted_sc):
            if self.sorted_sc[iter][0] in ignore_questions:
                iter+=1
                continue
            else:
                break

        coverage = list(set(self.attr_dic[self.sorted_sc[iter][0]]) - set(ignored_datasets))

        self.curr_question_iter = iter
        if iter >= len(self.sorted_sc):
            return None
        
        return (len(coverage), self.sorted_sc[iter][0])

    def ask_question_gui(self, question, df_lst):
        self.curr_question = question
        self.curr_question_iter += 1

        attrs = self.cluster_attr[question]
        display(Markdown('<h3><strong>{}</strong></h3>'.format("Does the required dataset contain any of these attributes:")))

        wordcloud = WordCloud(background_color="white",stopwords=STOPWORDS,collocations=True).generate(' '.join(attrs))
        def grey_color_func(word, font_size, position, orientation, random_state=None,
                    **kwargs):
            return "hsl(0, 0%%, %d%%)" % random.randint(0, 1)

        def update():
            plt.imshow(wordcloud.recolor(color_func=grey_color_func, random_state=3), cmap='Greys',interpolation='bilinear')
    
        wc_widget=widgets.interactive_output(update,{})
        display(wc_widget)

        self.attribute_yesno=widgets.RadioButtons(
            options=self.OPTIONS,
            value=self.OPTIONS[-1], # Defaults to 'pineapple'
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

        if answer == 0:
            self.shortlist_func(self.attr_dic[self.curr_question])
        elif answer == 1:
            self.ignore_func(self.attr_dic[self.curr_question])

        self.submit_callback()
        return
    
    def ask_question(self, question, df_lst, submit_callback):
        self.submit_callback = submit_callback
        self.ask_question_gui(question, df_lst)
        return
    
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

    
