#!/usr/bin/env python
# coding: utf-8




import timeit

import math
import warnings
warnings.filterwarnings('ignore')
import tqdm
from Ver_DataFrame import Ver_DataFrame
# In[2]:


import ipywidgets as widgets
from IPython.display import clear_output
import pandas as pd
from datetime import datetime


from IPython.display import display
import ipywidgets as widgets
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import gensim
import sys



data_id=sys.argv[1]#'jp_1020'
query_path='/home/cc/queries1/'
#root_path='/home/cc/user_study'
root_path='/home/cc/output_views_small/'+data_id#/Users/sainyam/Documents/XMind'#'/home/cc/user_study'
data_path=root_path+'/home/cc/output_views_small/'+data_id#jp_1020'
candidate_path='/home/cc/zhiru/aurum-dod-staging/DoD/new_ver_4c/results/sainyam'



def read_query(query_path,data_id):
    query=''
    f=open(query_path+data_id+'.csv')
    for line in f:
        line=line.strip()
        line=line.split(',')
        query+=' '.join(line)
    return query

def get_4c_pairs(data_dic,candidate_path,data_id):
    f=open(candidate_path+'/compl_contra_pairs_'+data_id+'.txt')
    c_dic={}
    for line in f:
        line=line.strip()
        line=line.split(',')
        v1=data_dic[line[0].strip()]
        v2=data_dic[line[1].strip()]
        c_dic[(v1,v2)]=line[2:]
    return c_dic

fout=open("log-file-systema.txt",'a')
query=read_query(query_path,data_id)


start = timeit.default_timer()
model = gensim.models.KeyedVectors.load_word2vec_format('./GoogleNews-vectors-negative300.bin', binary=True)

def get_mean_vector(word2vec_model, words):
    # remove out-of-vocabulary words
    words=words.split()
    words = [word for word in words if word in word2vec_model]
    if len(words) >= 1:
        return np.mean(word2vec_model[words], axis=0)
    else:
        return []
    


def read_df(candidate_path,data_id):

    nonzero_lst=[]


    f=open(candidate_path+'/candidate_compl_views_'+data_id+'.txt','r')
    for line in f:
        line=line.strip()
        nonzero_lst.append(line)#[line[0]]=1#float(line[1])

    df_lst=[]
    attr_lst=[]
    attr_map={}
    df_name_lst=[]
    attr_to_val={}
    candidate_name_to_id={}
    s4_score={}
    i=1
    for d in nonzero_lst:# i<3500:
        try:
            df=Ver_DataFrame(pd.read_csv(d))
            df.set_name(d)
            candidate_name_to_id[d]=len(df_lst)
            df_str= (df.to_string().lower())
            df_name_lst.append(d)
            if True:#"connecticut" in df_str or "georgia" in df_str or "virginia" in df_str or "illinois" in df_str or "indiana" in df_str:
                attr_lst.extend(list(df.columns))
                df_lst.append(df)
                s4_score[len(df_lst)-1]=1#nonzero[d]
                for attr in list(df.columns):
                    if len(attr)<3:
                        continue
                    l=[]
                    val_lst=[]
                    if attr in attr_map.keys():
                        l=attr_map[attr]
                        val_lst=attr_to_val[attr]
                    val_lst.extend(list(df[attr]))
                    l.append(len(df_lst)-1)
                    attr_map[attr]=l
                    attr_to_val[attr]=val_lst
        #
        except:
            j=i#print (i)
        i+=1
    return attr_map,attr_to_val,attr_lst,df_lst,df_name_lst,s4_score,candidate_name_to_id

(attr_map,attr_to_val,attr_lst,df_lst,df_name_lst,s4_score,candidate_name_to_id)=read_df(candidate_path,data_id)
print (candidate_name_to_id)
c_dic=get_4c_pairs(candidate_name_to_id,candidate_path,data_id)
read_time = timeit.default_timer()-start
candidate_lst=df_lst

candidate_attributes=attr_lst
attribute_to_dataset=attr_map
wordcloud_lst=[]
#print (attribute_to_dataset)
import copy
import numpy as np
attr_lst=list(attr_map.keys())
attr_lst=list(set(attr_lst))


lst=[]
for attr in attr_lst:
    orig=copy.deepcopy(attr)
    attr=attr.replace('_',' ')
    attr=attr.replace(',',' ')
    attr=attr.replace('/',' ')
    lst.append(attr)
    attribute_to_dataset[attr]=attribute_to_dataset[orig]
    
attr_lst=lst
newlst=[]
for attr in attr_lst:
    if len(get_mean_vector(model,attr))==0:
        continue
    else:
        newlst.append(attr)
attr_lst=newlst
from collections import Counter
original_attr_to_val={}
for attr in attr_to_val.keys():
    lst=attr_to_val[attr]
    string=''
    for v in lst:
        string+=str(v)+" "
    string=string.replace('-',' ')
    string=string.replace(',',' ')
    string=string.replace('_',' ')
    string=string.replace('/',' ')
    original_attr_to_val[attr]=string
    split_it = string.split()
    Ctr = Counter(split_it)
    most_occur = Ctr.most_common(50)
    new_str=''
    for (u,v) in most_occur:
        new_str+=u+' '
    attr_to_val[attr]=new_str
    
attr_val_lst=[]
for attr in attr_to_val.keys():
    if len(get_mean_vector(model,attr_to_val[attr]))==0:
        continue
    else:
        attr_val_lst.append(attr)


# In[8]:


import random
#random.seed(1)
def cluster(attr_lst,attr_to_val,attr_val_lst,option):
    if option==0:
        attrs=attr_lst
    else:
        attrs=attr_val_lst
    import random
    i=0
    k=30
    centers=[]
    center_val=[]
    nr_points=len(attrs)
    max_dist_ind=random.randint(0,len(attrs)-1)
    pred = [None] * len(attrs)#nr_points
    import math
    distance_to_sln = [None] * nr_points
    new_point_dist=0
    max_dist=0
    while i<k:

        centers.append(max_dist_ind)
        center_val.append(attrs[max_dist_ind])
        #print (max_dist, centers[i],attrs[centers[i]])    


        max_dist = 0
        max_dist_ind = 0
        #print (i)
        j=0
        while j<len(attrs):
            #print (j)
            if i>=0:
                if centers[i]==j:
                    new_point_dist=0
                else:
                    if option==0:
                        new_point_dist=model.wmdistance(attrs[centers[i]].split(),attrs[j].split())
                    else:
                        if attrs[centers[i]] in attr_to_val.keys() and attrs[j] in attr_to_val.keys():
                            new_point_dist=model.wmdistance(attr_to_val[attrs[centers[i]]].split(),attr_to_val[attrs[j]].split())
                        else:
                            new_point_dist=0
                    
                        #new_point_dist=max(new_point_dist,new_point_dist1)
                if math.isinf(new_point_dist):
                    new_point_dist=2
            #if j<10 and i>0:
            #    print (new_point_dist,centers[i],centers[pred[j]])
            if i == 0 or new_point_dist < distance_to_sln[j]:
                pred[j] = i
                distance_to_sln[j] = new_point_dist


            if distance_to_sln[j] > max_dist:
                max_dist = distance_to_sln[j]
                max_dist_ind = j
            j+=1


        i+=1
    iter=0
    pred_map={}
    for attr in attr_val_lst:
        pred_map[attr] = center_val[pred[iter]]
        iter+=1
    return centers,center_val,pred,max_dist,pred_map


#Ranking of clusters is this


# In[9]:



def get_distance(query,attr_dic):
    scores={}
    for attr in attr_dic.keys():
        dist=model.wmdistance(attr_dic[attr].split(),query.split())
        scores[attr]=dist
    sorted_sc=sorted(scores.items(), key=lambda item: item[1],reverse=False)
    return sorted_sc

def get_distance_attribute(query,attr_dic):
    scores={}
    for attr in attr_dic.keys():
        dist=model.wmdistance(attr.split(),query.split())
        scores[attr]=dist
    sorted_sc=sorted(scores.items(), key=lambda item: item[1],reverse=False)
    return sorted_sc



def get_distance_df(query,df_lst):
    scores={}
    iter=0
    while iter< len(df_lst):
        #print (iter)
        df=df_lst[iter]
        collst=list(df.columns)
        mindist=1000
        for col in collst:
            vallst=(list(df[col].values))
            colstring=''
            for v in vallst:
                colstring+= str(v)+" "
            dist=model.wmdistance(colstring.split(),query.split())
            if dist<mindist:
                mindist=dist
        
        scores[iter]=mindist
        iter+=1
    sorted_sc=sorted(scores.items(), key=lambda item: item[1],reverse=False)
    return sorted_sc



#ranking of datasets using headers
def get_distance_df_header(query,df_lst):
    scores={}
    iter=0
    while iter< len(df_lst):
        #print (iter)
        df=df_lst[iter]
        collst=list(df.columns)
        mindist=1000
        for col in collst:
            dist=model.wmdistance(col.split(),query.split())
            if dist<mindist:
                mindist=dist
        
        scores[iter]=mindist
        iter+=1
    sorted_sc=sorted(scores.items(), key=lambda item: item[1],reverse=False)
    return sorted_sc


print (attr_lst)
print (attr_to_val)
print (attr_val_lst)

centers_content,center_content,pred1,max_dist,pred_map_content=cluster(attr_lst,attr_to_val,attr_val_lst,1)
cluster_content={}
for attr in pred_map_content.keys():
    l=[]
    if pred_map_content[attr] in cluster_content.keys():
        l=cluster_content[pred_map_content[attr]]
    l.append(attr)
    cluster_content[pred_map_content[attr]]=l
    


centers_attr,center_attr,pred,max_dist,pred_map_attr=cluster(attr_lst,attr_to_val,attr_val_lst,0)
cluster_attr={}
for attr in pred_map_attr.keys():
    l=[]
    if pred_map_attr[attr] in cluster_attr.keys():
        l=cluster_attr[pred_map_attr[attr]]
    l.append(attr)
    cluster_attr[pred_map_attr[attr]]=l
    

distance_lst_dataset_attribute=get_distance_df_header(query,df_lst)
distance_lst_dataset_content=get_distance_df(query,df_lst)
#Ranking of attributes using content
distance_lst_attribute_content=get_distance(query,attr_to_val)
#Ranking of attributes
distance_lst_attribute_header=get_distance_attribute(query,attr_to_val)

# In[10]:


#print (pred_map_content)


# In[11]:


pruned_lst=[]
shortlisted_lst=[]


# In[ ]:





# In[12]:


from collections import Counter
import numpy as np
def get_top_k(lst):
    word_lst=[]
    for l in lst:
        word_lst.extend(l)
    '''
    #word_lst=' '.join(word_lst).split(' ')
    counts = Counter(word_lst)

    labels, values = zip(*counts.items())

    # sort your values in descending order
    indSort = np.argsort(values)[::-1]

    # rearrange your data
    labels = np.array(labels)[indSort]
    values = np.array(values)[indSort]
    '''
    return word_lst#labels[:k],values[:k]


# In[13]:


import random,pickle


# In[14]:


#with open('candidates.pickle', 'rb') as handle:
#    [candidate_lst,candidate_attributes,attribute_to_dataset] = pickle.load(handle)


# In[15]:



shown_df_dic={}

def get_es_results(query_str,searchtype):
    
    threshold=20
    query_words=query_str.split(',')
    if searchtype=='Header':
        data_lst=[]
        header_lst=[]
        header_string=''
        '''query_body = {
                  "regexp": {
                      "header": "*"+qw+"*|*school*"
                  }
              }
        res = es.search(index="nyc-header-index", query=query_body,size=1000, from_=0)
        '''

        qstring=''
        iter=0
        for qw in query_words:
            qw=qw.strip()
            if iter<len(query_words)-1:
                qstring+="*"+qw+"* OR"
            else:
                qstring+="*"+qw+"*"
            iter+=1
        query_body = {
                  "match": {
                      "header": qstring
                  }
              }

            #Todo: Add next button for search results
        res = es.search(index="nyc-header-index", query=query_body,size=1000, from_=0)
        data_lst,header_lst,header_string = get_datasets_headers(res)
        #data_lst,header_lst,header_string = get_datasets_headers(res)
        
        df_lst=[]
        df_text=[]
        for (data_id,name) in data_lst:
            if len(df_lst)>threshold:
                break
            if name in shown_df_dic.keys():
            #Option 1
                (df_content,currdf) = shown_df_dic[name]
                if currdf.shortlistval<0:
                    continue
                df_lst.append(currdf)
                df_text.append(df_content)
            else:
                
                df_content = es.search(index="nyc-df-index", query= {"match":{'id': data_id }})['hits']['hits'][0]['_source']['df']
                df_text.append(df_content)
                currdf=Ver_DataFrame(pd.read_json(df_content))
            
                #Option 2:
                #df=pd.read_csv(data_path+name)
                #df_text.append(df.to_string())
                currdf.set_name(name)
                df_lst.append(currdf)
                shown_df_dic[name]=(df_content,currdf)
        #print (len(df_lst))
        return (data_lst,header_lst,header_string,df_lst,df_text)
    elif searchtype=='Content':  
        #TODO: 
        data_lst=[]
        header_lst=[]
        header_string=''
        '''query_body = {
                  "regexp": {
                      "header": "*"+qw+"*|*school*"
                  }
              }
        res = es.search(index="nyc-header-index", query=query_body,size=1000, from_=0)
        '''

        qstring=''
        iter=0
        for qw in query_words:
            qw=qw.strip()
            if iter<len(query_words)-1:
                qstring+="*"+qw+"* OR"
            else:
                qstring+="*"+qw+"*"
            iter+=1
        query_body = {"bool":{"must":{"query_string": {'query': "*"+qstring+"*" }}}}

            #Todo: Add next button for search results
        res = es.search(index="nyc-df-index", query=query_body,size=1000, from_=0)
        #res = es.search(index="nyc-df-index", query= {"bool":{"must":{"query_string": {'query': "*"+query_str+"*" }}}})
        data_lst = get_datasets(res)
        
        
        df_lst=[]
        df_text=[]
        for (data_id,name) in data_lst[:threshold]:
            if name in shown_df_dic.keys():
            #Option 1
                (df_content,currdf) = shown_df_dic[name]
                df_lst.append(currdf)
                df_text.append(df_content)
            else:
                
                df_content = es.search(index="nyc-df-index", query= {"match":{'id': data_id }})['hits']['hits'][0]['_source']['df']
                df_text.append(df_content)
                currdf=Ver_DataFrame(pd.read_json(df_content))
            
                #Option 2:
                #df=pd.read_csv(data_path+name)
                #df_text.append(df.to_string())
                currdf.set_name(name)
                df_lst.append(currdf)
                shown_df_dic[name]=(df_content,currdf)
            #Option 1
            '''
            df_content = es.search(index="nyc-df-index", query= {"match":{'id': data_id }})['hits']['hits'][0]['_source']['df']
            df_text.append(df_content)
            currdf=XMindDataFrame(pd.read_json(df_content))
            
            #Option 2:
            #df=pd.read_csv(data_path+name)
            #df_text.append(df.to_string())
            currdf.set_name(name)
            df_lst.append(currdf)
            '''
            
        return (data_lst,[],'',df_lst,df_text)

    else:
        #TODO:
        res = es.search(index="nyc-df-index", query= {"bool":{"must":{"query_string": {'query': "*"+query_str+"*" }}}})
        data_lst = get_datasets(res)
        
        
        df_lst=[]
        df_text=[]
        for (data_id,name) in data_lst[:threshold]:
            
            #Option 1
            df_content = es.search(index="nyc-df-index", query= {"match":{'id': data_id }})['hits']['hits'][0]['_source']['df']
            df_text.append(df_content)
            currdf=VerDataFrame(pd.read_json(df_content))
            
            #Option 2:
            #df=pd.read_csv(data_path+name)
            #df_text.append(df.to_string())
            currdf.set_name(name)
            df_lst.append(currdf)
            
        return (data_lst,[],'',df_lst,df_text)
        
    return get_datasets_headers(res)

def get_datasets(res):
    hit_arr=res['hits']['hits']
    data_lst=[]
    header_lst=[]
    header_string=''
    for val in hit_arr:
        #print (val)
        obj=val['_source']
        #print(obj['id'],obj['name'])
        data_lst.append((obj['id'],obj['name']))
    return data_lst


def get_datasets_headers(res):
    hit_arr=res['hits']['hits']
    data_lst=[]
    header_lst=[]
    header_string=''
    for val in hit_arr:
        #print (val)
        obj=val['_source']
        #print(obj['id'],obj['name'])
        data_lst.append((obj['id'],obj['name']))
        header_lst.append(obj['header'])
        header_string+= " "+" ".join(obj['header'])
    return data_lst,header_lst,header_string


def process_text(text):
    text=text.replace(':',' ')
    text=text.replace('-',' ')
    text=text.replace('_',' ')
    return text


# In[16]:


#print(len(candidate_lst))


# In[17]:


attribute_size={}
for attr in attribute_to_dataset.keys():
    attribute_size[attr]=len(attribute_to_dataset[attr])


# In[18]:



from tqdm import tqdm

from ipywidgets import interactive
from ipywidgets import Layout, Button, Box, FloatText, Textarea, Dropdown, Label, IntSlider

import copy
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import operator
from IPython.display import HTML

HTML('<style> .widget { width: -10; } </style>')

#List of candidates
original_lst=[]

turn_on_wordcloud=False

class interface:
    def __init__(self):
        global fout
        start = timeit.default_timer()
        fout.write("logging-started"+str(start)+"\n")
        
        self.ranking_score={}
        self.recommend_question=True
        self.initial_prob=[1.0,1.0,1.0,1.0,1.0,1.0]
        self.answered={}
        self.asked={}
        global pruned_lst,shortlisted_lst,wordcloud_lst,distance_lst_dataset_attribute,cluster_attr,original_attr_to_val
        pruned_lst=[]
        self.queried_centers=[]
        shortlisted_lst=[]
        self.queried_datasets=[]
        self.shortlisted_attr=[]
        self.discarded_attr=[]
        self.interface_options=['wordcloud','attribute','dataset','wordcloud-cont','attribute-cont','dataset-cont','4c']#,'pattern']
        for interface in self.interface_options:
            self.answered[interface]=0
            self.asked[interface]=0
        self.num_questions=[0,0,0,0]
        
        self.rank_search_querybox=widgets.Text(
                value='',
                placeholder='Type something',
                description='Enter Query:',
                disabled=False)
        self.rank_button = widgets.Button(
                description='Submit',
                disabled=False,
                button_style='', # 'success', 'info', 'warning', 'danger' or ''
                tooltip='Submit',
                icon='' # (FontAwesome names without the `fa-` prefix)
            )
        self.querybox=widgets.Text(
                value='',
                placeholder='Type something',
                description='Enter Query:',
                disabled=False)
        self.wordcloud=widgets.Checkbox(
                value=True,
                description='WordCloud',
                disabled=False,
                indent=False
            )
        self.start = widgets.Button(
                description='Start',
                disabled=False,
                button_style='', # 'success', 'info', 'warning', 'danger' or ''
                tooltip='Submit',
                icon='' # (FontAwesome names without the `fa-` prefix)
            )
        self.notfound = widgets.Button(
                description='End Task',
                disabled=False,
                button_style='', # 'success', 'info', 'warning', 'danger' or ''
                tooltip='Submit',
                icon='' # (FontAwesome names without the `fa-` prefix)
            )
        self.button = widgets.Button(
                description='Submit',
                disabled=False,
                button_style='', # 'success', 'info', 'warning', 'danger' or ''
                tooltip='Submit',
                icon='' # (FontAwesome names without the `fa-` prefix)
            )
        self.shortlist_all=widgets.Button(
                description='Shortlist all',
                disabled=False,
                button_style='', # 'success', 'info', 'warning', 'danger' or ''
                tooltip='Submit',
                icon='' # (FontAwesome names without the `fa-` prefix)
            )
        self.show_shortlist = widgets.Button(
                description='Show Shortlisted Datasets',
                disabled=False,
                button_style='', # 'success', 'info', 'warning', 'danger' or ''
                tooltip='Submit',
                icon='' # (FontAwesome names without the `fa-` prefix)
            )
        self.skip = widgets.Button(
                description='Skip',
                disabled=False,
                button_style='', # 'success', 'info', 'warning', 'danger' or ''
                tooltip='Submit',
                icon='' # (FontAwesome names without the `fa-` prefix)
            )
        self.recommend_ques = widgets.Button(
                description='Go to second stage',
                disabled=False,
                button_style='', # 'success', 'info', 'warning', 'danger' or ''
                tooltip='Submit',
                icon='' # (FontAwesome names without the `fa-` prefix)
            )
        self.back = widgets.Button(
                description='I want to enter keywords',
                disabled=False,
                button_style='', # 'success', 'info', 'warning', 'danger' or ''
                tooltip='Submit',
                icon='' # (FontAwesome names without the `fa-` prefix)
            )
        self.refresh = widgets.Button(
                description='Refresh',
                disabled=False,
                button_style='', # 'success', 'info', 'warning', 'danger' or ''
                tooltip='Submit',
                icon='' # (FontAwesome names without the `fa-` prefix)
            )
        self.dropdown=widgets.Dropdown(
            options=['Header', 'Content'],
            value='Header',
            description='Search over:',
            disabled=False,
        )
        
        self.yesno=widgets.RadioButtons(
            options=['yes', 'no'],
            value='no', # Defaults to 'pineapple'
        #    layout={'width': 'max-content'}, # If the items' names are long
            description='',
            disabled=False
        )
        self.attribute_yesno=widgets.RadioButtons(
            options=['Yes, my data must contain this attribute', 'No, the data should not contain this attribute'],
            value='No, the data should not contain this attribute', # Defaults to 'pineapple'
        #    layout={'width': 'max-content'}, # If the items' names are long
            description='',
            disabled=False
        )
        self.queried_attributes=[]
        self.notfound.on_click(self.endtask)
        self.button.on_click(self.submit_ques)
        self.start.on_click(self.start_clicked)
        self.shortlist_all.on_click(self.shortlist)
        self.skip.on_click(self.skip_ques)
        self.recommend_ques.on_click(self.ask_question)
        self.show_shortlist.on_click(self.show_shortlisted_data)
        self.back.on_click(self.first_stage)
        self.refresh.on_click(self.display_output)
        self.query_sequence=[]
        self.response_lst=[]
        self.start_study('')
        self.prev_output=''
        
        return
    def endtask(self,b):
        clear_output()
        print ("You have completed the task")
        start = timeit.default_timer()
        fout.write("Ending the task: "+str(start)+"\n")
        fout.close()
    def start_clicked(self,b):
        #Write to file
        start = timeit.default_timer()

        fout.write("start submitted:"+str(start)+"\n")
        self.display_output(b)
    def first_stage(self,b):
        self.recommend_question=0
        self.display_output(b)
    def shortlist(self,b):
        
        others_lst=list(candidate_name_to_id.keys())
        for df in self.prev_output[1]:
            shortlisted_lst.append(candidate_name_to_id[df._name])
            try:
                others_lst.remove(candidate_name_to_id[df._name])
            except:
                continue

        pruned_lst.extend(others_lst)
        #print (len(shortlisted_lst),len(pruned_lst))
        self.display_button(b)
    def get_attribute(self,attr_dic):
        sorted_size = sorted(attribute_size.items(), key=operator.itemgetter(1),reverse=True)
        for (attr,count) in sorted_size:
            if attr in self.shortlisted_attr or attr in self.discarded_attr or attr in self.queried_attributes:
                continue
            else:
                self.queried_attributes.append(attr)
                return attr
            
        #i=random.randint(0,len(combined)-1)
        #return combined[i]
    def process_text(self,txt):
        txt=txt.replace('_',' ')
        txt=txt.replace('-',' ')
        return txt
    def show_shortlisted_data(self,b):
        iter=0
        clear_output()

        
        df_widget_lst=[]
        iter=0
        for data_id in shortlisted_lst[:20]:
            #print (data_id)
            df=candidate_lst[data_id]
            
            #print (df.shortlistval)
            #dfwidget=qgrid.show_grid(df,column_definitions={ 'index': { 'maxWidth': 0, 'minWidth': 0, 'width': 0 } },grid_options={'forceFitColumns': False, 'defaultColumnHeight': 100,'defaultColumnWidth': 200, 'fullWidthRows': False,'syncColumnCellResize': False})
            dfwidget=setup_ui(df)
            
            
            dfwidget.layout = widgets.Layout(width='500px', height='300px',min_height='300px')
            dfwidget.add_class("left-spacing-class")
            
            #df.checkbox.add_class("checkbox-spacing-class")

            
            turn_on_wordcloud=True
            if turn_on_wordcloud:
                df_string=self.process_text((df.to_string()))
                def grey_color_func(word, font_size, position, orientation, random_state=None,
                    **kwargs):
                    return "hsl(0, 0%%, %d%%)" % random.randint(0, 1)

                wordcloud = WordCloud(background_color="white",stopwords=STOPWORDS,collocations=True).generate(df_string)
            
                def update():
                    #plt.imshow(wordcloud.recolor(color_func=grey_color_func, random_state=3), cmap='Greys',interpolation='bilinear')
                    plt.imshow(wordcloud, cmap='Greys',interpolation='bilinear')

                wc_widget=widgets.interactive_output(update,{})
                wc_widget.layout = widgets.Layout(width='500px', height='250px',min_height='250px',margin_left='500px')
                wc_widget.add_class("wcwidget-class")
                df.throwbutton.add_class('throw-class')
                #singledf=widgets.VBox([name,widgets.HBox([df.checkbox,dfwidget])],layout=Layout(min_height='350px'))
                singledf=widgets.VBox([widgets.HBox([dfwidget,widgets.VBox([wc_widget,df.selectbutton,df.throwbutton],layout=Layout(align_items='center', margin='0px 0px 0px 0px'))])],layout=Layout(min_height='350px'))
            else:
                singledf=widgets.VBox([widgets.HBox([widgets.VBox([df.throwbutton,df.selectbutton,df.throwbutton]),dfwidget])],layout=Layout(min_height='350px'))
            df_widget_lst.append(singledf)
            iter+=1
        
        #display(widgets.HBox([self.querybox,self.button,widgets.VBox([setup_ui(output[0]),setup_ui(output[1])])]))

        display(HTML(
         "<style>.left-spacing-class {margin-left: 5px; width:300px}</style>"
        ))
        display(HTML(
         "<style>.wcwidget-class {margin-left: 55px}</style>"
        ))
        display(HTML(
         "<style>.throw-class {margin-top: -10px}</style>"
        ))
        
        #display(HTML(
        # "<style>.checkbox-spacing-class {margin-left: -80px;}</style>"
        #))
        HTML('<style> .widget-text { width: -10} </style>')
        display(widgets.HBox([widgets.VBox(children=df_widget_lst,layout=Layout(
                border='3px solid black',
                width='900px',
                height='1100px',
                min_height='300px',
                flex_flow='column',
                display='flex'
            ))]))#,widgets.VBox([self.throwbutton,self.shortlistbutton])]))


    def get_question(self,interface):
        ques_score=1
        if interface=='4c':
            #use c_dic
            return 0,None
        if 'wordcloud' in interface:
            coverage=[]
            if interface=='wordcloud':
                lst=[]
                for c in center_attr:
                    if not c in cluster_attr.keys():
                        continue
                    if c in self.queried_centers:
                        continue
                    else:
                        self.queried_centers.append(c)
                        lst.append(c)

                        lst.extend(cluster_attr[c])
                        break
                final_lst={}
                coverage=[]
                for l in lst:

                    if l in self.shortlisted_attr or l in self.discarded_attr or l in self.queried_attributes:
                        continue
                    else:
                        final_lst[l]=1
                        print (l,"l is ")
                        coverage.extend(attribute_to_dataset[l])
            else:
                lst=[]
                for c in center_content:
                    if not c in cluster_content.keys():
                        continue
                    if c in self.queried_centers:
                        continue
                    else:
                        self.queried_centers.append(c)
                        lst.append(c)

                        lst.extend(cluster_content[c])
                        break
                final_lst={}
                for l in lst:
                    
                    if l in self.shortlisted_attr or l in self.discarded_attr or l in self.queried_attributes:
                        continue
                    else:
                        final_lst[l]=1
                        coverage.extend(attribute_to_dataset[l])
            if len(list(final_lst.keys()))==0:
                return 0,None
            #word_lst=get_top_k(candidate_lst)
            return len(list(set(coverage))),final_lst#word_lst
            #Pick top 10 attributes and their frequencies
        if 'attribute' in interface:
            coverage=[]
            if interface=='attribute':
                for(d,sc) in distance_lst_attribute_header:
                    if d in self.queried_attributes:
                        continue
                    #print (d,sc)
                    self.queried_attributes.append(d)
                    coverage.extend(attribute_to_dataset[d])
                    return len(coverage),d
            else:
                for(d,sc) in distance_lst_attribute_content:
                    if d in self.queried_attributes:
                        continue
                    #print (d,sc)
                    coverage.extend(attribute_to_dataset[d])
                    self.queried_attributes.append(d)
                    return len(coverage),d
            return 0,None
           
        if 'dataset' in interface:
            if interface=='dataset':
                min_sc=100
                options=[]
                for(d,sc) in distance_lst_dataset_attribute:
                    if d in self.queried_datasets:
                        continue
                    else:
                        if sc < min_sc:
                            min_sc=sc
                        elif sc > min_sc:
                            break
                        options.append(d)
                maxval=0   
                return_d=0
                random.shuffle(options)
                for d in options:
                    if s4_score[d]>maxval:
                        return_d=d
                        maxval=s4_score[d]
                self.queried_datasets.append(return_d)
                #print ("this",return_d,options,maxval)
                if len(options)==0:
                    return 0,None
                return 1,[df_lst[return_d],return_d]
            else:
                min_sc=100
                options=[]
                for(d,sc) in distance_lst_dataset_content:
                    if d in self.queried_datasets:
                        continue
                    else:
                        if sc < min_sc:
                            min_sc=sc
                        elif sc > min_sc:
                            break
                        options.append(d)
                maxval=0   
                return_d=0
                random.shuffle(options)
                for d in options:
                    if s4_score[d]>maxval:
                        return_d=d
                        maxval=s4_score[d]
                self.queried_datasets.append(return_d)
                #print ("this",return_d,options,maxval)
                if len(options)==0:
                    return 0,None
                return 1,[df_lst[return_d],return_d]
            
           
            
            
    def choose_interface(self):
        
        #Threshold to switch to bandit based approach
        threshold=math.ceil(math.log(len(self.interface_options))*1.0/math.log(2))
        gamma=0.1

        #print ("threshold",threshold)
        scores=[]
        corresponding_ques=[]
        iter=0
        choose_random=False
        valid_interfaces=[]
        for interface in self.interface_options:
            #Get reduction in score and corresponding question for each interface
            #score: \chi(I)
            #print ("interface is ",interface)
            score,ques=self.get_question(self.interface_options[iter])


            answer_prob=1
            if self.asked[interface]<threshold:
                choose_random=True
            else:
                answer_prob=self.answered[interface]*1.0/self.asked[interface]
            print ("interface",interface,answer_prob,score)
            #this is w(I)
            if ques is not None:
                answer_prob=0
                valid_interfaces.append(interface)
                scores.append(answer_prob*score)
                corresponding_ques.append(ques)
            iter+=1
        print ("choosing",scores,self.interface_options,choose_random,self.asked,threshold)
        if len(valid_interfaces)==0:
            return None,None
        if choose_random:
            max_index=random.randint(0,len(scores)-1)#scores.index(max(scores))
            print (max_index,len(scores))
        else:
            #Toss a coin with prob. TODO
            total_score=sum(scores)
            i=0
            randval=random.random()

            for interface in valid_interfaces:
                scores[i]=(1-gamma)*scores[i]*1.0/total_score + gamma*1.0/len(valid_interfaces)
                if randval<scores[i]:
                    break
                randval-=scores[i]
                i+=1
            max_index=i
        print (valid_interfaces,len(valid_interfaces)) 
        return corresponding_ques[max_index],valid_interfaces[max_index]
    
    def update(self,wc):
        plt.imshow(wc,cmap='Greys', interpolation='bilinear')

    def get_datasets(self):
        return original_lst
    def skip_ques(self,b):
        start = timeit.default_timer()
        fout.write("skip is hit:"+str(start)+"\n")
        self.response_lst.append("skip")
        self.display_output(b)
    def ask_question(self,b):
        self.recommend_question=True
        self.response_lst.append("skip")
        self.display_output(b)
    def start_study(self,b):
        display(self.start)
    def submit_ques(self,b):
        start = timeit.default_timer()
        fout.write("submit is hit:"+str(start)+"\n")
        global shortlisted_lst,pruned_lst
        self.response_lst.append("submit")
        
        if len(self.query_sequence)>0:
            start = timeit.default_timer()
            itype=self.query_sequence[-1][0]
            fout.write("prev output:"+str(itype)+"\n")
            #print (itype,self.yesno.value,"A")
            if 'dataset' in itype:
                fout.write("prev output:"+str(self.yesno.value)+"\n")
                if self.yesno.value=='no':
                    data_id=self.query_sequence[-1][-1][-1]
                    pruned_lst.append(data_id)
                    if data_id in self.ranking_score.keys():
                        self.ranking_score[data_id]-=1
                    else:
                        self.ranking_score[data_id]=-1
                elif self.yesno.value=='yes':
                    data_id=self.query_sequence[-1][-1][-1]
                    if data_id in self.ranking_score.keys():
                        self.ranking_score[data_id]+=1
                    else:
                        self.ranking_score[data_id]=1
                    shortlisted_lst.append(data_id)
                else:
                    self.response_lst[-1]="skip"
                
            #Remove data from candidate list
            if 'attribute' in itype:
                fout.write("prev output:"+str(self.attribute_yesno.value)+"\n")
                if 'No' in self.attribute_yesno.value:
                    pruned_lst.extend(attribute_to_dataset[self.query_sequence[-1][-1]])
                    for data_id in attribute_to_dataset[self.query_sequence[-1][-1]]:
                        if data_id in self.ranking_score.keys():
                            self.ranking_score[data_id]-=1
                        else:
                            self.ranking_score[data_id]=-1
                    self.discarded_attr.append(self.query_sequence[-1][-1])
                elif 'Yes' in self.attribute_yesno.value:
                    shortlisted_lst.extend(attribute_to_dataset[self.query_sequence[-1][-1]])
                    for data_id in attribute_to_dataset[self.query_sequence[-1][-1]]:
                        if data_id in self.ranking_score.keys():
                            self.ranking_score[data_id]+=1
                        else:
                            self.ranking_score[data_id]=1
                    self.shortlisted_attr.append(self.query_sequence[-1][-1])
                else:
                    self.response_lst[-1]="skip"
              
            if 'wordcloud' in itype:#='wordcloud':
                out=self.wc_radio.value
                fout.write("prev output:"+str(out)+"\n")
                if 'unrelated' in out:
                    #print ("he")
                    lst=list(self.query_sequence[-1][-1].keys())
                    self.discarded_attr.extend(lst)
                    for attr in lst:
                        pruned_lst.extend(attribute_to_dataset[attr])
                        for data_id in attribute_to_dataset[attr]:
                            if data_id in self.ranking_score.keys():
                                self.ranking_score[data_id]-=1
                            else:
                                self.ranking_score[data_id]=-1
                elif 'related' in out and 'All' in out:
                    lst=list(self.query_sequence[-1][-1].keys())
                    self.shortlisted_attr.extend(lst)
                    for attr in lst:
                        shortlisted_lst.extend(attribute_to_dataset[attr])
                        for data_id in attribute_to_dataset[attr]:
                            if data_id in self.ranking_score.keys():
                                self.ranking_score[data_id]-=1
                            else:
                                self.ranking_score[data_id]=-1
                    
            if itype in self.answered.keys():
                self.answered[itype]+=1
            self.display_output(b)
        else:
            self.display_output(b)

    def display_ranking(self):
        print ("Shortlisted datasets are listed below")
        display(self.refresh)
        df_widget_lst=[]
        iter=0
        #print (len(shortlisted_lst))
        #print (self.ranking_score)
        sorted_sc = sorted(self.ranking_score.items(), key=operator.itemgetter(1),reverse=True)
        for data_id,sc in tqdm(sorted_sc):
            #print (data_id)
            if data_id not in shortlisted_lst:
                continue
            df=candidate_lst[data_id]
            
            #print (df.shortlistval)
            #dfwidget=qgrid.show_grid(df,column_definitions={ 'index': { 'maxWidth': 0, 'minWidth': 0, 'width': 0 } },grid_options={'forceFitColumns': False, 'defaultColumnHeight': 100,'defaultColumnWidth': 200, 'fullWidthRows': False,'syncColumnCellResize': False})
            dfwidget=setup_ui(df)
            
            
            dfwidget.layout = widgets.Layout(overflow='scroll',width='500px', height='300px',min_height='300px')
            dfwidget.add_class("left-spacing-class")
            
            #df.checkbox.add_class("checkbox-spacing-class")

            
            turn_on_wordcloud=True
            if turn_on_wordcloud:
                df_string=self.process_text((df.iloc[:100].to_string()))
                def grey_color_func(word, font_size, position, orientation, random_state=None,
                    **kwargs):
                    return "hsl(0, 0%%, %d%%)" % random.randint(0, 1)

                #wordcloud = wordcloud_lst[data_id]#
                df_string=df_string.replace('NaN','')
                df_string=df_string.replace('NoNe','')
                wordcloud=WordCloud(background_color="white",stopwords=STOPWORDS,collocations=True).generate(df_string)
            
                def grey_color_func(word, font_size, position, orientation, random_state=None,
                    **kwargs):
                    return "hsl(0, 0%%, %d%%)" % random.randint(0, 1)

                def update():
                    plt.imshow(wordcloud.recolor(color_func=grey_color_func, random_state=3), cmap='Greys',interpolation='bilinear')
                    #plt.imshow(wordcloud, cmap='Greys',interpolation='bilinear')

                wc_widget=widgets.interactive_output(update,{})
                wc_widget.layout = widgets.Layout(width='500px', height='250px',min_height='250px',margin_left='500px')
                wc_widget.add_class("wcwidget-class")
                df.throwbutton.add_class('throw-class')
                #singledf=widgets.VBox([name,widgets.HBox([df.checkbox,dfwidget])],layout=Layout(min_height='350px'))
                singledf=widgets.VBox([widgets.HBox([dfwidget,widgets.VBox([wc_widget,df.selectbutton,df.throwbutton],layout=Layout(align_items='center', margin='0px 0px 0px 0px'))])],layout=Layout(min_height='350px'))
            else:
                singledf=widgets.VBox([widgets.HBox([widgets.VBox([df.throwbutton,df.selectbutton,df.throwbutton]),dfwidget])],layout=Layout(min_height='350px'))
            df_widget_lst.append(singledf)
            iter+=1
        
        #display(widgets.HBox([self.querybox,self.button,widgets.VBox([setup_ui(output[0]),setup_ui(output[1])])]))

        display(HTML(
         "<style>.left-spacing-class {margin-left: 5px; width:300px}</style>"
        ))
        display(HTML(
         "<style>.wcwidget-class {margin-left: 55px}</style>"
        ))
        display(HTML(
         "<style>.throw-class {margin-top: -10px}</style>"
        ))
        
        #display(HTML(
        # "<style>.checkbox-spacing-class {margin-left: -80px;}</style>"
        #))
        HTML('<style> .widget-text { width: -10} </style>')
        display(widgets.HBox([widgets.VBox(children=df_widget_lst,layout=Layout(
                border='3px solid black',
                width='900px',
                height='1100px',
                min_height='300px',
                flex_flow='column',
                display='flex'
            ))]))#,widgets.VBox([self.throwbutton,self.shortlistbutton])]))


    def display_output(self,b):
        global candidate_lst,candidate_attributes,attribute_to_dataset,shortlisted_lst,pruned_lst
        clear_output()
        start = timeit.default_timer()
        #print ("Input Examples: Connecticut, Georgia, Virginia, Illinois, Indiana")

        fout.write("next output displayed:"+str(start)+"\n")
        df_lst=[]
        header_string=''
        #todo: Read the output of previous interaction
        #and update the options
        
        #TODO: Identify the best interface
        interface_type='search'
        #print (self.response_lst)#,self.pruned_lst)
        if self.recommend_question:
            (ques,interface_type)=self.choose_interface()
            fout.write("current question:"+str(interface_type))
            self.asked[interface_type]+=1
            #print (interface_type)
        
        

        
        #TODO: Show that interface and other output

        #TODO: Create different tabs of interfaces
        
        turn_on_wordcloud=self.wordcloud.value
        
        #Identify the query
        #Have multiple types of keyword search: Headers, cell values
        if interface_type=='search':
            #global querybox
            #print ("Query output\n\n",self.querybox.value,self.dropdown.value)
            if self.querybox.value != "":
                (data_lst,header_lst,header_string,df_lst,df_text)=get_es_results(self.querybox.value,self.dropdown.value)
                
            self.query_sequence.append(["search",self.querybox.value])
            
            keyword_search=widgets.HBox([self.querybox,self.button,self.dropdown,self.wordcloud])
            display(keyword_search)
            display(self.recommend_ques)
            #display(self.querybox,self.button)
        elif interface_type=='cluster':
            #Shows a wordcloud
            text='hi i am sainyam i am here i am going to here here here'
            wordcloud = WordCloud(background_color="white",stopwords=STOPWORDS).generate(text)
            #print (wordcloud)
            def update():
                plt.imshow(wordcloud, cmap='Greys',interpolation='bilinear')

            wc_widget=widgets.interact(update)
            display(widgets.HBox([wc_widget]))
            #TODO: Give a drop down to ignore this cluster
            # Select a cluster
            #Rank
        elif interface_type=='attribute_lst':
            #User can choose specific attributes that seem useful
            #We can choose how many attributes user wants to see!!!!!
            #TODO: Choose attributes to show
            options_lst=['borough','state','city']
            print ("Choose attributes that you want in the dataset")
            w_lst=[]
            for attr in options_lst:
                wc=widgets.Checkbox(
                    value=False,
                    description=attr,
                    disabled=False,
                    indent=False
                )
                w_lst.append(wc)
            #selection_wc.on_click(self.display_output)
            display(widgets.VBox(w_lst))
            display(self.button)
        elif interface_type=='wordcloud':
            print ("Would you shortlist a dataset containing any of these attributes?")
            self.wc_radio=widgets.RadioButtons(
                options=['All these attributes are unrelated to the query', 'All are related to the query','Some are related to the query'],
            #    value='pineapple', # Defaults to 'pineapple'
            #    layout={'width': 'max-content'}, # If the items' names are long
                description='',
                disabled=False
            )
            fout.write(" :"+str(ques)+"\n")
#            wordcloud1 = WordCloud(background_color="white",max_words=10,stopwords=STOPWORDS).generate(text1)
            wordcloud = WordCloud(background_color="white",max_words=3,stopwords=STOPWORDS).generate_from_frequencies(ques)
            
            def grey_color_func(word, font_size, position, orientation, random_state=None,
                    **kwargs):
                    return "hsl(0, 0%%, %d%%)" % random.randint(0, 1)

            def update():
                plt.imshow(wordcloud.recolor(color_func=grey_color_func, random_state=3), cmap='Greys',interpolation='bilinear')
                #plt.imshow(wordcloud1, cmap='Greys',interpolation='bilinear')
            ck1=widgets.Checkbox(
                    value=False,
                    description='',
                    disabled=False,
                    indent=False
                )
            
            wc_widget1=widgets.interactive_output(update,{})
            
            display(widgets.HBox([wc_widget1,widgets.VBox([self.wc_radio,self.button,self.skip])]))
            #display(self.back)
            self.query_sequence.append(["wordcloud",ques])
            #self.display_ranking()
        elif interface_type=='wordcloud_pair':
            #Two sets of dataframes and we want to generate different statistics to show
            print ("Which topics would you prefer?")
            text1='hi i am sainyam i am here i am going to here here here'
            text2='hi i am sainyam i am here i am going to here here here'
            
            wordcloud1 = WordCloud(background_color="white",stopwords=STOPWORDS).generate(text1)
            wordcloud2 = WordCloud(background_color="white",stopwords=STOPWORDS).generate(text2)

            def update():
                plt.imshow(wordcloud1, cmap='Greys',interpolation='bilinear')
            def update2():
                plt.imshow(wordcloud2, cmap='Greys',interpolation='bilinear')
            ck1=widgets.Checkbox(
                    value=False,
                    description='',
                    disabled=False,
                    indent=False
                )
            ck2=widgets.Checkbox(
                    value=False,
                    description='',
                    disabled=False,
                    indent=False
               )
            #Todo: Create a global list of ck and use those!

            wc_widget1=widgets.interactive_output(update,{})
            wc_widget2=widgets.interactive_output(update2,{})

            display(widgets.HBox([ck1,wc_widget1,ck2,wc_widget2]))
            
            print ("wc")
        elif interface_type=='attribute':
            print ("Do you want to shortlist datasets containing the attribute:'"+ques+"'?")
            
            #print (attribute_to_dataset[ques][:5])
            fout.write(" :"+str(ques)+"\n")
            string=original_attr_to_val[ques].split()
            count=Counter(string)
            sorted_size = sorted(count.items(), key=operator.itemgetter(1),reverse=True)
            final_lst={}
            iter=0

            for (attr,count) in sorted_size:
                if 'nan' in attr or 'none' in attr:
                    continue
                final_lst[attr]=count
                if iter>10:
                    break
            #print (final_lst)  
               
            wordcloud = WordCloud(background_color="white",max_words=10,stopwords=STOPWORDS).generate_from_frequencies(final_lst)
            
            def grey_color_func(word, font_size, position, orientation, random_state=None,
                    **kwargs):
                    return "hsl(0, 0%%, %d%%)" % random.randint(0, 1)

            def update():
                plt.imshow(wordcloud.recolor(color_func=grey_color_func, random_state=3), cmap='Greys',interpolation='bilinear')
                #plt.imshow(wordcloud1, cmap='Greys',interpolation='bilinear')
            
            
            wc_widget=widgets.interactive_output(update,{})
            display(wc_widget)
            display(widgets.HBox([self.attribute_yesno,self.button,self.skip]))
            self.query_sequence.append(["attribute",ques])        
            #display(self.back)
            #self.display_ranking()
        elif interface_type=='dataset':
            print ("Would you shortlist this dataset for the query?")
            df=ques[0]
            fout.write(" :"+str(df.to_string())+"\n")
            
            #dfwidget=qgrid.show_grid(df,column_definitions={ 'index': { 'maxWidth': 0, 'minWidth': 0, 'width': 0 } },grid_options={'forceFitColumns': False, 'defaultColumnHeight': 100,'defaultColumnWidth': 200, 'fullWidthRows': False,'syncColumnCellResize': False})
            dfwidget=setup_ui(df)
            
            
            dfwidget.layout = widgets.Layout(overflow = 'scroll',width='700px', height='300px',min_height='300px')
            dfwidget.add_class("left-spacing-class")
            
            display(widgets.HBox([dfwidget,self.yesno,self.button,self.skip]))
            self.query_sequence.append(["dataset",ques])
            #display(self.back)
            
        elif interface_type=='pairwise_question':
            print ("Choose attributes that you want in the final dataset?")
            
            
            print ("Which dataset would you prefer?")
            #Propagate results to nearby datasets!
            print ("Which attribute do you prefer?")
            print ("Which value do you prefer for XYZ attribute?")
            display(self.yesno)
        elif interface_type=='pattern_search':
            print ("Does any of the attributes satisfy this pattern?")
            #print ("pattern")
            #Give pattern and some attributes as example
            
            
        #elif interface_type=='topic cluster':
        #elif interface_type=='ranklist':
        
        #elif interface_type=='niffler':
            #As user to enter tokens that are definitely present
            #maybe present
            #should not be present
        display(HTML('<hr size="8" color="black">'))
        display(HTML(
         "Could not find any dataset:"))
        display(self.notfound)
        display(HTML('<hr size="8" color="black">'))
        self.display_ranking()  
            
        #TODO: Blow code shows the output on the screen
                
        
        #Write code to display a set of datframes and allow user to remove certain dataframes from the list
        #Give a checkbox maybe
        if interface_type=='search':
            found=False
            if len(self.query_sequence)>1:
                if self.query_sequence[-1]==self.query_sequence[-2]:
                    found=True

            if found:
                print ("Number of shortlisted datasets",len(shortlisted_lst))
                print ("Number of removed datasets",len(pruned_lst))
                widget_lst=[]
                [df_widget_lst,df_lst]=self.prev_output
                iter=0
                new_df_lst=[]
                for df in tqdm(df_lst):
                    if df.shortlistval==-1:
                        iter+=1
                        continue
                    else:
                        widget_lst.append(df_widget_lst[iter])
                        new_df_lst.append(df)
                    iter+=1
                df_widget_lst=widget_lst
                df_lst=new_df_lst
            else:
                df_widget_lst=[]
                iter=0
                if len(df_lst)>0:
                    new_df_lst=[]
                    df_widget_lst=[]
                    for df in tqdm(df_lst):
                        name=widgets.HTMLMath(
                            value=df._name
                        )
                        #print (df.shortlistval)
                        dfwidget=qgrid.show_grid(df,column_definitions={ 'index': { 'maxWidth': 0, 'minWidth': 0, 'width': 0 } },grid_options={'forceFitColumns': False, 'defaultColumnHeight': 100,'defaultColumnWidth': 200, 'fullWidthRows': False,'syncColumnCellResize': False})
                        #dfwidget=setup_ui(df)

                        if df.shortlistval==-1:
                            continue
                        new_df_lst.append(df)
                        dfwidget.layout = widgets.Layout(width='500px', height='300px',min_height='300px')
                        dfwidget.add_class("left-spacing-class")




                        #df.checkbox.add_class("checkbox-spacing-class")

                        df_string=df_text[iter]#process_text(process_text(df.to_string()))

                        if turn_on_wordcloud:
                            def grey_color_func(word, font_size, position, orientation, random_state=None,
                                **kwargs):
                                return "hsl(0, 0%%, %d%%)" % random.randint(0, 1)

                            wordcloud = WordCloud(background_color="white",stopwords=STOPWORDS,collocations=True).generate(df_string)

                            def update():
                                #plt.imshow(wordcloud.recolor(color_func=grey_color_func, random_state=3), cmap='Greys',interpolation='bilinear')
                                plt.imshow(wordcloud, cmap='Greys',interpolation='bilinear')

                            wc_widget=widgets.interactive_output(update,{})
                            wc_widget.layout = widgets.Layout(width='500px', height='250px',min_height='250px',margin_left='500px')
                            wc_widget.add_class("wcwidget-class")
                            df.throwbutton.add_class('throw-class')
                            #singledf=widgets.VBox([name,widgets.HBox([df.checkbox,dfwidget])],layout=Layout(min_height='350px'))
                            singledf=widgets.VBox([name,widgets.HBox([dfwidget,widgets.VBox([wc_widget,df.throwbutton,df.shortlistbutton],layout=Layout(align_items='center', margin='0px 0px 0px 0px'))])],layout=Layout(min_height='350px'))
                        else:
                            singledf=widgets.VBox([name,widgets.HBox([widgets.VBox([df.throwbutton,df.shortlistbutton]),dfwidget])],layout=Layout(min_height='350px'))
                        df_widget_lst.append(singledf)

                        iter+=1

                    #display(widgets.HBox([self.querybox,self.button,widgets.VBox([setup_ui(output[0]),setup_ui(output[1])])]))

                    display(HTML(
                     "<style>.left-spacing-class {margin-left: 5px; width:300px}</style>"
                    ))
                    display(HTML(
                     "<style>.wcwidget-class {margin-left: 55px}</style>"
                    ))
                    display(HTML(
                     "<style>.throw-class {margin-top: -10px}</style>"
                    ))

                    #display(HTML(show
                    # "<style>.checkbox-spacing-class {margin-left: -80px;}</style>"
                    #))
                    HTML('<style> .widget-text { width: -10} </style>')

                    #display(widgets.HBox(df_widget_lst))

            if self.querybox.value != "":
                '''s
                header_wordcloud = WordCloud(background_color="white",stopwords={}).generate(header_string)
                def update():
                    plt.imshow(header_wordcloud, interpolation='bilinear')


                wc_widget=widgets.interactive_output(update,{})
                df_widget_lst.append(wc_widget)
                '''
                print ("Number of shortlisted datasets",len(shortlisted_lst))
                print ("Number of removed datasets",len(pruned_lst))
                display(widgets.VBox([self.shortlist_all,self.refresh,widgets.VBox(children=df_widget_lst[:10],layout=Layout(
                    overflow='scroll hidden',
                    border='3px solid black',
                    width='900px',
                    height='1100px',
                    min_height='300px',
                    flex_flow='column',
                    display='flex'
                ))]))#,widgets.VBox([self.throwbutton,self.shortlistbutton])]))
                self.prev_output=[df_widget_lst,new_df_lst]
                #if len(self.querybox.value) > 0 : 
                #Need to implement its effect
                #keyword_search=widgets.HBox([self.querybox,self.button,setup_ui(display_dfs(output))])
                #display(keyword_search)



    #Todo:
    #1. indexing (Done but need to integrate with Aurum! and show join paths)
    #2: [Done] Remove stop words, numerical columns and -,. 
    #3: Add a tab with shortlisted options
    #Todo: Show top10 instead of wordcloud



#Implement a user that answers everything correctly!
#Go over shortlisted list and if one element is left then we are done
#Initially all datasets are the search space and after shortlisted is non empty they become the search space.
#TODO: Implement a mechanism to show questions from shortlisted datasets too!!!!!
setup_time=timeit.default_timer()-start
ver_int=interface()
i=0
total=0
while i<100:
    start = timeit.default_timer()
    (ques,itype)= (ver_int.choose_interface())
    end_time = timeit.default_timer()
    total+=(end_time-start)
    if ques==None:
        break
    print (i,ques,itype)
    ver_int.asked[itype]+=1

    #user response from simulation
    user_response=False
    if 'wordcloud' in itype:
        lst=list(ques.keys())
        if not user_response:
            ver_int.discarded_attr.extend(lst)
            for attr in lst:
                pruned_lst.extend(attribute_to_dataset[attr])
        else:
            ver_int.shortlisted_attr.extend(lst)
            for attr in lst:
                shortlisted_lst.extend(attribute_to_dataset[attr])
    i+=1
print (setup_time,read_time)
print ("Averaage time per query",total*1.0/i)
