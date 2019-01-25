#!/usr/bin/env python
# coding: utf-8

# In[1]:


from __future__ import print_function
import os, sys
from pyspark import SparkContext
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
from decimal import Decimal
from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
import numpy as np
from pyspark.mllib.clustering import KMeans, KMeansModel, BisectingKMeans, BisectingKMeansModel
from collections import (defaultdict, Counter)
import heapq
import operator
import json


# In[2]:


conf = SparkConf().setAppName("App")
conf = (conf.setMaster('local[*]')
        .set('spark.executor.memory', '4G')
        .set('spark.driver.memory', '45G')
        .set('spark.driver.maxResultSize', '10G'))
sc = SparkContext(conf=conf)


# In[72]:


fileName = str(sys.argv[1])
k = int(sys.argv[3])
maximumNumberOfIteration = int(sys.argv[4])
algorithm = str(sys.argv[2])


# In[73]:


# with open(fileName, "r") as fh:
#     linesOfTrainData = fh.readlines()
# print("Line count in training :" ,len(linesOfTrainData))


# In[74]:


# training_list = []
# document_id_dictionary = {}
# id_document_dictionary = {}
# i = 0
# for td in linesOfTrainData:
#     td = td.rstrip(" \r\n")
#     training_list.append(td)
#     if td not in document_id_dictionary:
#         document_id_dictionary[td] = i
#         id_document_dictionary[i] = td
#         i= i + 1
#     else:
#         td = td + "."
#         document_id_dictionary[td] = i
#         id_document_dictionary[i] = td
#         i= i + 1
#         print(td)


# In[75]:


# fields = sc.parallelize(document_id_dictionary.keys())
# documents = fields.flatMap(lambda x: x.split(" ")).map(lambda x:((x,1))).reduceByKey(add)


# In[76]:


# wordsList = documents.collect()


# In[77]:


# fields1 = sc.parallelize(document_id_dictionary.values())
# documentId = fields1.map(lambda x: x)


# In[78]:


# documentId.collect()


# In[79]:


documents1 = sc.textFile(fileName).map(lambda line: line.split(" "))
hashingTF = HashingTF(50000)
tf = hashingTF.transform(documents1)


# In[80]:


# wordIdDictionary = {}
# for word in wordsList:
#     wordIdDictionary[hashingTF.indexOf(word[0])] = word[0]

# wordIdDictionary


# In[81]:


# tf.collect()


# In[82]:


tf.cache()
idf = IDF(minDocFreq=1).fit(tf)


# In[83]:


tfidf = idf.transform(tf)


# In[84]:


# tfidf.collect()


# In[85]:


if algorithm == "K":
    clusters = KMeans.train(tfidf, 8, maxIterations=20, initializationMode="random", seed=42)
else:
    clusters = BisectingKMeans.train(tfidf, 8, maxIterations=20, seed=42)
    clusterCenters = clusters.clusterCenters



# In[ ]:





# In[86]:


documentModel = documents1.zip(tfidf)
# cluster_broadcast = sc.broadcast(clusters)


# In[87]:


def findErrorWC(document,clusters):
    documentWords = document[0]
    documentTfidf = document[1]
    #find cluster that document belongs to
    predictedClusterID = clusters.predict(documentTfidf)
    #clustser cenroid of relevant document
    clusterCenter = clusters.clusterCenters[predictedClusterID]
#     print(clusterCenter)
    documentTfidf = np.array(documentTfidf)
    documentError = np.sum(np.square(documentTfidf - clusterCenter))
    wordCounter = Counter
    #word count
    documentWordCount = wordCounter(documentWords)
#     print(predictedClusterID)
    #return word count, error and number of docs
    return (predictedClusterID, (documentWordCount,documentError,1))

#     print(documentWordCount)


# In[88]:


def KfindErrorWC(document,clusters):
    documentWords = document[0]
    documentTfidf = document[1]
    #find cluster that document belongs to
    documentTfidf = np.array(documentTfidf)
    clusterDistanceList = []
    clusterDistanceSUMList = []
    clusterCenter = 0
    for cluster in clusters:
        documentDistancesum = np.sum(np.square(documentTfidf - cluster))
        documentDistancesumSq = np.sqrt(documentDistancesum)
        clusterDistanceList.append(documentDistancesumSq)
        clusterDistanceSUMList.append(documentDistancesum)
        documentDistancesum = 0
        documentDistancesumSq = 0

    predictedClusterID = clusterDistanceList.index(min(clusterDistanceList))
    clusterCenter = cluster[predictedClusterID]
#     print(clusterDistanceList)
#     print(predictedClusterID)
    #clustser cenroid of relevant document

#     print(clusterCenter)

    documentError =clusterDistanceSUMList[predictedClusterID]
    wordCounter = Counter
    #word count
    documentWordCount = wordCounter(documentWords)
#     print(predictedClusterID)
    #return word count, error and number of docs
    return (predictedClusterID, (documentWordCount,documentError,1))

#     print(documentWordCount)


# In[89]:


if algorithm == "K":
    document_error_wc_rdd = documentModel.map(lambda x: findErrorWC(x,clusters))
else:
    document_error_wc_rdd = documentModel.map(lambda x: KfindErrorWC(x,clusterCenters))


# In[90]:


# documentModel.collect()
#Merge documents in same cluster
sum_document_cluster = document_error_wc_rdd.reduceByKey(lambda r1, r2: (r1[0] + r2[0], r1[1] + r2[1], r1[2] + r2[2]))


# In[91]:


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


# In[92]:


# sum_rdd.collect()
topWords_error_rdd = sum_document_cluster.mapValues(lambda x:((topWords(x[0])),x[1],x[2]))


# In[93]:


final_list = topWords_error_rdd.collect()


# In[94]:


# sum_document_cluster.collect()
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


# In[95]:


json_final = createJSONfile(final_list,algorithm)


# In[96]:


# json_final
json_final_write = OrderedDict()
json_final_write["algorithm"] = json_final["algorithm"]
json_final_write["WSSE"] = json_final["WSSE"]
json_final_write["clusters"] = json_final["clusters"]


# In[97]:


if algorithm == "K":
    op_fileName = "./Parth_Vaghani_Cluster_small_K_" + str(k) + "_" + str(maximumNumberOfIteration) + ".json"
else:
    op_fileName = "./Parth_Vaghani_Cluster_small_B_"+ str(k) + "_" + str(maximumNumberOfIteration) + ".json"
with open(op_fileName, 'w') as f:
    json.dump(json_final_write, f)


# In[98]:


op_fileName
# len(clusters.centers[0])


# In[31]:


# hashingTF.indexOf("remeniscent")


# In[32]:


# predictions=clusters.predict(tfidf)


# In[33]:


# predictions.count()


# In[34]:


# predictions.collect()


# In[35]:


# a = predictions.map(lambda x: ((x,1)))


# In[36]:


# a.reduceByKey(add).collect()


# In[37]:


# b = a.collect()


# In[38]:


# for i in range(0,len(b)):
#     print(i)


# In[39]:


# WSSE = clusters.computeCost(tfidf)


# In[40]:


# print(WSSE)


# In[41]:


# def tfcal(x):
#     print(x)


# In[42]:


# temp1 = tfidf.map(lambda x:tfcal(x)).collect()


# In[43]:


# temp1.collect()


# In[44]:


# clusters.centers[0].computeCost(tfidf)


# In[ ]:
