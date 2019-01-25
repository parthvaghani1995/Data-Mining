#!/usr/bin/env python
# coding: utf-8

# In[44]:


from __future__ import print_function
import os, sys
from operator import add
import itertools
import time
import copy
import math
from operator import itemgetter
from collections import Counter
from itertools import combinations
import random
from collections import OrderedDict
from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
import numpy as np
from collections import (defaultdict, Counter)
import operator
import json
from pyspark.mllib.linalg import Vectors, SparseVector, DenseVector


# In[2]:


conf = SparkConf().setAppName("App")
conf = (conf.setMaster('local[*]')
        .set('spark.executor.memory', '4G')
        .set('spark.driver.memory', '45G')
        .set('spark.driver.maxResultSize', '10G'))
sc = SparkContext(conf=conf)


# In[103]:

# "/Users/parth/Desktop/USC/Data Mining/Assignment4/Data/yelp_reviews_clustering_small.txt"
fileName = str(sys.argv[1])
k = int(sys.argv[3])
maximumNumberOfIteration = int(sys.argv[4])
feature = str(sys.argv[2])


# In[104]:


with open(fileName, "r") as fh:
    linesOfTrainData = fh.readlines()
print("Line count in training :" ,len(linesOfTrainData))


# In[105]:


training_list = []
document_id_dictionary = {}
id_document_dictionary = {}
i = 0
for td in linesOfTrainData:
    td = td.rstrip(" \r\n")
    training_list.append(td)
    if td not in document_id_dictionary:
        document_id_dictionary[td] = i
        id_document_dictionary[i] = td
        i= i + 1
    else:
        td = td + "."
        document_id_dictionary[td] = i
        id_document_dictionary[i] = td
        i= i + 1
#         print(td)



# In[106]:


documents1 = sc.textFile(fileName).map(lambda line: line.split(" "))
hashingTF = HashingTF(50000)
tf = hashingTF.transform(documents1)


# In[107]:


tf.cache()
if feature == "W":
    print(feature)
    tfidf = tf.map(lambda x:x)
else:
    idf = IDF(minDocFreq=1).fit(tf)
    tfidf = idf.transform(tf)


# In[136]:


# a = tfidf.collect()
# b = tfidf
# b.collect()
# PythonRDD[12] at RDD at PythonRDD.scala:52


# In[109]:


documentModel = documents1.zip(tfidf)
random.seed(20181031)


# In[110]:


a = tfidf.collect()
# a


# In[111]:


id_feature_dictionary = {}
feature_id_dictionary = {}
for i in range(0,len(a)):
    id_feature_dictionary[i] = a[i]
    feature_id_dictionary[a[i]] = i


# In[112]:


feature_id_dictionary_bc = sc.broadcast(feature_id_dictionary)


# In[113]:


randomNumberList = []

while len(randomNumberList)<k:
    generatedNumber = int(random.uniform(0,(len(id_document_dictionary))))
    if generatedNumber not in randomNumberList:
        randomNumberList.append(generatedNumber)

# for i in range(0,k):
#     randomNumberList.append()#length of document
# i = 0


# In[137]:


# randomNumberList


# In[115]:


centroidList = []
for i in range(0,k):
    centroidList.append(id_document_dictionary[randomNumberList[i]])
#     print(randomNumberList[i])
# (centroidList)


# In[116]:


centroidList_rdd = sc.parallelize(centroidList)
centroid_rdd = centroidList_rdd.map(lambda x: x.split(" "))
# len(centroid_rdd.collect())


# In[117]:


tf_centroid = hashingTF.transform(centroid_rdd)
tf_centroid.cache()
if feature == "W":
    tfidf_centroid = tf_centroid.map(lambda x:x).collect()
else:
    idf_centroid = IDF(minDocFreq=1).fit(tf_centroid)
    tfidf_centroid = idf_centroid.transform(tf_centroid).collect()


# In[118]:


# (tfidf_centroid)


# In[119]:


id_feature_dictionary_rdd = sc.parallelize(id_feature_dictionary)
id_document_dictionary_bc = sc.broadcast(id_feature_dictionary)


# In[120]:


def centroidDistance(document,centroid,doc_dic):
    documentID = document
    returnDistanceDictionary = {}
    distanceDictionary = {}
    document_values = doc_dic[documentID]
#     documentTfidf = np.array(document_values)
#     print(documentTfidf)
    for i in range(0,len(centroid)):
        distanceMeasure = document_values.squared_distance(centroid[i])
        distanceMeasureSqrt = np.sqrt(distanceMeasure)
        distanceDictionary[i] = distanceMeasureSqrt
#         centroidtfidf = np.array(centroid[i])
#         documentDistancesum = np.sum(np.square(documentTfidf - centroidtfidf))
#         print(distanceMeasureSqrt)
#     print(distanceDictionary)
    minimumClusterNumber = min(distanceDictionary, key=distanceDictionary.get)
#     print(minimumClusterNumber)
    returnDistanceDictionary[documentID] = minimumClusterNumber
#     print(returnDistanceDictionary)

#     print(document_values)
#     print("\n\n\n\n")
    return tuple((minimumClusterNumber,documentID))



# In[121]:


centroid_calculation_rdd = id_feature_dictionary_rdd.map(lambda x: centroidDistance(x,tfidf_centroid,id_document_dictionary_bc.value)).groupByKey().map(lambda x : (x[0], list(x[1]))).collect()
centroid_calculation_rdd.sort(key=lambda tup: tup[0])
# centroid_calculation_rdd
previousVectorList = centroid_calculation_rdd[:]
# print(previousVectorList)


# In[122]:


#Function to add 2 sparse vectors
def add(v1, v2):
    assert isinstance(v1, SparseVector) and isinstance(v2, SparseVector)
    assert v1.size == v2.size
    indices = set(v1.indices).union(set(v2.indices))
    v1d = dict(zip(v1.indices, v1.values))
    v2d = dict(zip(v2.indices, v2.values))
    zero = np.float64(0)
    values =  {i: v1d.get(i, zero) + v2d.get(i, zero)
       for i in indices
       if v1d.get(i, zero) + v2d.get(i, zero) != zero}

    return Vectors.sparse(v1.size, values)


# In[123]:


def dividev(v1, n):
    indices = set(v1.indices)
    v1d = dict(zip(v1.indices, v1.values))
#     print(v1d)
    zero = np.float64(0)
#     print(zero)
    values =  {i: v1d.get(i, zero) / n
       for i in indices
       if v1d.get(i, zero) / n != zero}
    return Vectors.sparse(v1.size, values)


# In[124]:


for i in range(2,maximumNumberOfIteration):
    updated_Centroid_Dictionary = {}
    updated_Centroid_list = []
#     print(centroid_calculation_rdd)
    for clusterNumber in range(0,k):
#         print(clusterNumber)
        cluster_group_tuple = centroid_calculation_rdd[clusterNumber]
#         print("OK")
        numberOfDocs = len(cluster_group_tuple[1])
        current_centroid_value = id_feature_dictionary[cluster_group_tuple[1][0]]
        for docu in range(1,numberOfDocs):
            current_centroid_value = add(current_centroid_value,id_feature_dictionary[cluster_group_tuple[1][docu]])

        current_centroid_value = dividev(current_centroid_value,numberOfDocs)
        updated_Centroid_list.append(current_centroid_value)
#         print(numberOfDocs)
#         print((current_centroid_value))

#         print(cluster_group_tuple)
#         print(current_centroid_value == id_feature_dictionary[32])
#         print(current_centroid_value)

#         print(numberOfDocs)
#     print(len(updated_Centroid_list))
    tfidf_centroid = updated_Centroid_list
    centroid_calculation_rdd = id_feature_dictionary_rdd.map(lambda x: centroidDistance(x,tfidf_centroid,id_document_dictionary_bc.value)).groupByKey().map(lambda x : (x[0], list(x[1]))).collect()
    centroid_calculation_rdd.sort(key=lambda tup: tup[0])
    currentVectorList = centroid_calculation_rdd[:]
    for katappa in centroid_calculation_rdd:
        print(len(katappa[1]))
    if (previousVectorList == currentVectorList):
        break
    else:
        previousVectorList = currentVectorList[:]

    print("\n\n\n")
# 20993: 2.277267285009756


# In[125]:


# updated_Centroid_Dictionary = {}
# updated_Centroid_list = []
# #     print(centroid_calculation_rdd)
# for clusterNumber in range(0,k):
# #         print(clusterNumber)
#     cluster_group_tuple = centroid_calculation_rdd[clusterNumber]
#     numberOfDocs = len(cluster_group_tuple[1])
#     current_centroid_value = id_feature_dictionary[cluster_group_tuple[1][0]]
#     for docu in range(1,numberOfDocs):
#         current_centroid_value = add(current_centroid_value,id_feature_dictionary[cluster_group_tuple[1][docu]])

#     current_centroid_value = dividev(current_centroid_value,numberOfDocs)
#     updated_Centroid_list.append(current_centroid_value)


# In[126]:


def findErrorWC(document,clusterList,centroidList,doc_dic,feat_id_dic):
    documentWords = document[0]
    documentTfidf = document[1]
    documentID = feat_id_dic[documentTfidf]
#     print(documentID)
    for i in range(0,len(clusterList)):
        if documentID in clusterList[i][1]:
            clusterID = i
            break
    centroid_values = centroidList[clusterID]
    documentError = documentTfidf.squared_distance(centroid_values)
    wordCounter = Counter
    #word count
    documentWordCount = wordCounter(documentWords)
    return (clusterID, (documentWordCount,documentError,1))
#     print(documentError)
#     print(documentTfidf)


# In[127]:


document_error_wc_rdd = documentModel.map(lambda x: findErrorWC(x,centroid_calculation_rdd,tfidf_centroid,id_document_dictionary_bc.value,feature_id_dictionary_bc.value))


# In[128]:


sum_document_cluster = document_error_wc_rdd.reduceByKey(lambda r1, r2: (r1[0] + r2[0], r1[1] + r2[1], r1[2] + r2[2]))


# In[129]:


def topWords(x):
    topWordsDictionary = []
    for word in x:
        topWordsDictionary.append(tuple((word,x[word])))
    a = topWordsDictionary.sort(key=lambda tup: tup[1], reverse=True)
#     print((topWordsDictionary))
    finalTopWords = []
    for i in range(0,10):
        finalTopWords.append(topWordsDictionary[i][0])

#     print(finalTopWords)
    return finalTopWords


# In[130]:


# sum_rdd.collect()
topWords_error_rdd = sum_document_cluster.mapValues(lambda x:((topWords(x[0])),x[1],x[2]))


# In[131]:


final_list = topWords_error_rdd.collect()


# In[132]:


def createJSONfile(x,algorithm):
    final_json = {}
    WSSESum = 0
    cluster_dic = OrderedDict()
    final_json["clusters"] = []
    for cluster in x:
        cluster_dic["id"] = cluster[0]
        cluster_dic["size"] = cluster[1][2]
        cluster_dic["error"] = cluster[1][1]
        cluster_dic["terms"] = cluster[1][0]
        WSSESum = WSSESum + cluster[1][1]
        final_json["clusters"].append(cluster_dic)
        cluster_dic= OrderedDict()
    final_json["WSSE"] = WSSESum
    if algorithm == "K":
        final_json["algorithm"] = "K-Means"
    else:
        final_json["algorithm"] = "Bisecting K-Means"

#         print(cluster_dic)
#     print(final_json)
#     print("\n\n\n")
    return final_json
#     print(x)


# In[133]:


json_final = createJSONfile(final_list,"K")


# In[134]:


json_final_write = OrderedDict()
json_final_write["algorithm"] = json_final["algorithm"]
json_final_write["WSSE"] = json_final["WSSE"]
json_final_write["clusters"] = json_final["clusters"]


# In[135]:


op_fileName = "./Parth_Vaghani_KMeans_small_" + feature + "_" + str(k) + "_" + str(maximumNumberOfIteration) + ".json"
with open(op_fileName, 'w') as f:
    json.dump(json_final_write, f)


# In[92]:





# In[93]:





# In[35]:


# v1 = Vectors.sparse(3, {0: 1.0, 2: 1.0})
# v2 = Vectors.sparse(3, {1: 1.0, 2: 3.0})
# b = add(id_feature_dictionary[0],id_feature_dictionary[1])


# In[36]:


# type(b)


# In[37]:


# c = dividev(v2,3)


# In[38]:


# c


# In[39]:


# centroid_calculation_rdd


# In[ ]:
