import random
import view_presentation.interface.embedding_distance as embedding_distance

def cluster(attr_lst,attr_to_val,attr_val_lst,option,embedding_obj,k):

    if option==0:
        attrs=attr_lst
    else:
        attrs=attr_val_lst
    
    i=0

    centers=[]
    center_val=[]
    if len(attrs)==0:
        return centers,center_val,[],0,{}
    nr_points=len(attrs)
    if k>nr_points:
        k=nr_points
    max_dist_ind=random.randint(0,len(attrs)-1)
    pred = [None] * len(attrs)#nr_points
    import math
    distance_to_sln = [None] * nr_points
    new_point_dist=0
    max_dist=0
    dontrun=False#True
    if dontrun==True:
        return centers,center_val,pred,-1,{}
    while i<k:
        
        centers.append(max_dist_ind)
        center_val.append(attrs[max_dist_ind])

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
                        new_point_dist=embedding_obj.get_distance(attrs[centers[i]],attrs[j])
                    else:
                        if attrs[centers[i]] in attr_to_val.keys() and attrs[j] in attr_to_val.keys():
                            new_point_dist=embedding_obj.get_distance(attr_to_val[attrs[centers[i]]],attr_to_val[attrs[j]])
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
    for attr in attrs:
        pred_map[attr] = center_val[pred[iter]]
        iter+=1
    return centers,center_val,pred,max_dist,pred_map
