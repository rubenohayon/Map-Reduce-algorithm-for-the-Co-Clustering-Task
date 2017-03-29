from __future__ import division
import random
import math


#Choose your parameters 
path = "/FileStore/tables/krfln45v1484046384131/ratings.dat" # Databricks file location
K=20
L=20
T=10

def find_userclusterid_itemclusterid(line):
    user_cluster_id = U.value.get((line[0])) 
    item_cluster_id= V.value.get((line[1][0]))
    x=((user_cluster_id,item_cluster_id),(line[1][1],1))
    return x
	
def find_itemclusterid(line):
    item_cluster_id=V.value.get((line[1][0]))
    x=((line[0]),[(item_cluster_id,line[1][1])])
    return x

def find_userclusterid(line):
    user_cluster_id=U.value.get((line[1][0]))
    x=((line[0]),[(user_cluster_id,line[1][1])])
    return x
  
def row_mapper(line):
  error=0
  list_error=[]
  for i in range (0,K):
    for values in line[1]:
      error+=math.pow(((values[1])-(B[i][values[0]])),2)
    list_error.append(error)
    error=0
  best_cluster=list_error.index(min(list_error))
  for values in line[1]:
    temp_B[best_cluster][values[0]][0]+=values[1]
    temp_B[best_cluster][values[0]][1]+=1
  return (line[0],best_cluster)

def column_mapper(line):
  error=0
  list_error=[]
  for i in range (0,L):
    for values in line[1]:
      error+=math.pow(((values[1])-(B[values[0]][i])),2)
    list_error.append(error)
    error=0
  best_cluster=list_error.index(min(list_error))
  for values in line[1]:
    temp_B[values[0]][best_cluster][0]+=values[1]
    temp_B[values[0]][best_cluster][1]+=1
  return (line[0],best_cluster)


file=sc.textFile(path)
#Read all the file  : U (uid,random_cluster), V (iid,random_cluster), , 
U=file.map(lambda x : (int(x.split("::")[0]))).distinct().map(lambda x: (x,random.randint(0, K-1)))
V=file.map(lambda x : (int(x.split("::")[1]))).distinct().map(lambda x: (x,random.randint(0, L-1)))
U = sc.broadcast(U.collectAsMap())
V = sc.broadcast(V.collectAsMap())
U_rating=file.map(lambda line: ((int(line.split("::")[0])),(int(line.split("::")[1]),int(line.split("::")[2])))) #U_rating (U,(iid,rating))
V_rating=file.map(lambda line: ( (int(line.split("::")[1])),(int(line.split("::")[0]),int(line.split("::")[2])) )) #V_rating (iid,(uid,rating))


uclid_iclid_rating = U_rating.map(find_userclusterid_itemclusterid) #(usercluser_id,itemclusterid,rating)
uid_iclid_rating = U_rating.map(find_itemclusterid).reduceByKey(lambda x, y: (x+y)) #(user_id,list of ((itemclusterid,rating)))
vid_uclid_rating =V_rating.map(find_userclusterid).reduceByKey(lambda x, y: (x+y)) #(item_id,list of ((userclusterid,rating)))


B = [[0 for x in range(L)] for y in range(K)] #initialize B with 0

avg_uclusterid_icluterid = uclid_iclid_rating.reduceByKey(lambda x, y: (x[0] + y[0],x[1] + y[1])).map(lambda x : ((x[0]),((x[1][0])/(x[1][1]))))
#(user_cluser_id,item_clusterId,average)

def update_B():
  for cell in avg_uclusterid_icluterid.collect():
    B[cell[0][0]][cell[0][1]] = cell[1] # average

update_B()

def recompute_B():
    for uclid in range(0,K):
        for iclid in range(0,L):
            if(temp_B[uclid][iclid][1]!=0):
              B[uclid][iclid] = (temp_B[uclid][iclid][0])/(temp_B[uclid][iclid][1])
            else:
              continue
    B_B = sc.broadcast(B)
t=1
while (t<T):
  
  temp_B = [[[0,0] for x in range(L)] for y in range(K)]
  
  U = uid_iclid_rating.map(row_mapper)
  U = sc.broadcast(U.collectAsMap()) #Update U 

  recompute_B()

  vid_uclid_rating =V_rating.map(find_userclusterid).reduceByKey(lambda x, y: (x+y)) #(item_id,userclusterid,rating)

  
  temp_B = [[[0,0] for x in range(L)] for y in range(K)]

  V = vid_uclid_rating.map(column_mapper)
  V = sc.broadcast(V.collectAsMap())

  uid_iclid_rating = U_rating.map(find_itemclusterid).reduceByKey(lambda x, y: (x+y)) #(user_id,list of ((itemclusterid,rating,rating^2)))

  recompute_B()
  
  t=t+1

print(B)
