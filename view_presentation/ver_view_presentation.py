import pandas as pd
import math, random

import view_presentation.interface.embedding_distance as embedding_distance
#from view_presentation.interface.attribute_content_interface import AttributeContentInterface
import view_presentation.config as config
import view_presentation.interface_list as interface_lst
import view_presentation.interface as int_folder

random.seed(config.seed)


class ViewPresentation:
    def __init__(self,df_lst):
        self.interface_options=[]
        self.asked={}
        self.answered={}

        embedding_obj = embedding_distance.EmbeddingModel()
        initialize_candidates()
        
        for interface in self.interface_options:
            self.answered[interface]=0
            self.asked[interface]=0
        

    #Add a function to initialize the candidates for each interface
    def initialize_candidates():
        for curr_interface in interface_lst.interface_options:
            aci = curr_interface("ainterface")
            aci.generate_candidates(df_lst)
            aci.rank_candidates("new york city",embedding_obj)
            self.interface_options.append(aci)
    
        

    # ranking of questions

    def choose_interface(self):

        threshold=math.ceil(math.log(len(self.interface_options))*1.0/math.log(2))
        
        
        gamma=config.gamma

        scores=[]
        corresponding_ques=[]
        iter=0
        choose_random=False
        valid_interfaces=[]
        coverage_lst=[]

        for interface in self.interface_options:#move to config.py
            
            score,ques,coverage=interface.get_question()

            answer_prob=1
            if ques is not None and  self.asked[interface]<threshold:
                choose_random=True
            elif ques is not None:
                answer_prob=self.answered[interface]*1.0/self.asked[interface]
            #this is w(I)
            if ques is not None:
                answer_prob=0
                valid_interfaces.append(interface)
                scores.append(answer_prob*score)
                corresponding_ques.append(ques)
                coverage_lst.append(coverage)
            iter+=1
        if len(valid_interfaces)==0:
            return None,None,None
        if choose_random:
            max_index=random.randint(0,len(scores)-1)#scores.index(max(scores))
        else:
            #Toss a coin with prob. TODO
            total_score=sum(scores)
            i=0
            randval=random.random()

            for interface in valid_interfaces:
                print (i)
                if total_score>0:
                    scores[i]=(1-gamma)*scores[i]*1.0/total_score + gamma*1.0/len(valid_interfaces)
                else:
                    scores[i]=1.0/len(valid_interfaces)
                if randval<scores[i]:
                    break
                randval-=scores[i]
                i+=1
            max_index=i
        print ("valid",valid_interfaces,len(valid_interfaces))
        #TODO: update asked question for the interface

        '''
        if "wordcloud" in valid_interfaces[max_index]:
            self.queried_centers.append(list(corresponding_ques[max_index].keys()))
        elif "attribute" in valid_interfaces[max_index]:
            self.queried_attributes.append(corresponding_ques[max_index])
        elif "dataset" in valid_interfaces[max_index]:
            self.queried_datasets.append(corresponding_ques[max_index][1])
        elif "4c" in valid_interfaces[max_index]:
            self.queried_datasets.extend(coverage_lst[max_index])
        '''
        return corresponding_ques[max_index],valid_interfaces[max_index],coverage_lst[max_index]
    


#example usage of the interface
if __name__ == '__main__':

    data1 = ["Chicago","NYC","SF","Seattle"]
    df1 = pd.DataFrame(data1, columns=['City'])
  
    data2 = ["Paris","Copenhagen","Delhi","Sydney"]
    df2 = pd.DataFrame(data2, columns=['international city'])

    df_lst=[df1,df2]

    vp = ViewPresentation(df_lst)
    vp.choose_interface()

    '''
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

    '''