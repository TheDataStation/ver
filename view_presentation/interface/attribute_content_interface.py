import pandas as pd
from interface import interface
from attribute_name_interface import AttributeNameInterface
import embedding_distance

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


if __name__ == '__main__':  
    data1 = ["Chicago","NYC","SF","Seattle"]
    df1 = pd.DataFrame(data1, columns=['City'])
  
    data2 = ["Paris","Copenhagen","Delhi","Sydney"]
    df2 = pd.DataFrame(data2, columns=['international city'])

    df_lst=[df1,df2]

   
    print ("Content interface")
    attr_inf=AttributeContentInterface("header content interface")
    attr_inf.generate_candidates(df_lst)
    print(attr_inf.attr_dic)

    embedding_obj = embedding_distance.EmbeddingModel()
    print(attr_inf.rank_candidates("USA cities",embedding_obj))


    print(attr_inf.get_question())

