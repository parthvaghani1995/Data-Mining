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


# In[2]:


sc = SparkContext(appName="task2")


# In[3]:


# fileName = "/Users/parth/Desktop/USC/Data Mining/Assignment3/inf553_assignment3/Data/yelp_reviews_large.txt"
fileName = str(sys.argv[1])

support = float(sys.argv[2])
trainData = sc.textFile(fileName,None,False)


# In[4]:


start = time.time()
# trainData.collect()


# In[5]:


baskets_rdd = trainData.map(lambda x: x.split(",")).map(lambda x: (int(x[0]),x[1])).groupByKey().mapValues(set).sortByKey(True)
numberOfBaskets = baskets_rdd.count()


# In[6]:


onlyBaskets_rdd = baskets_rdd.map(lambda x: x[1])
# onlyBaskets_rdd.collect()


# In[7]:


def frequentItemsGenerator(basket, candidatePairs, partitionThreshold):
    countDictionary = {}
    for candidate in candidatePairs:
#         print(type(candidate))
        candidate = set(candidate)
        temp1 = sorted(candidate) #make sure that the candidate is sorted
        tupleCandidate = tuple(temp1)
#         print(temp2)
#         print(candidate)
        for item in basket:
            if candidate.issubset(item):
                if tupleCandidate in countDictionary:
                    countDictionary[tupleCandidate] = countDictionary[tupleCandidate] + 1
                else:
                    countDictionary[tupleCandidate] = 1
#     print(countDictionary)
    kItemCount = Counter(countDictionary)
    frequentItemSetDictionary = {x : kItemCount[x] for x in kItemCount if kItemCount[x] >= partitionThreshold }
#     for item in countDictionary:
#         if countDictionary[item]>=partitionThreshold:
#             frequentItemSetDictionary[item] = countDictionary[item]
#     print(frequentItemSetDictionary)
    frequentItemSet = sorted(frequentItemSetDictionary)
#     print(frequentItemSet)
#     print(basket)
    return frequentItemSet


# Phase 1 Map

# In[8]:


def candidateItemsGenerator(frequentItem,s):
    combo = list()
    frequentItem = list(frequentItem)
    for i in range(len(frequentItem) - 1):
        for j in range(i+1, len(frequentItem)):
            temp1 = frequentItem[i]
            temp2 = frequentItem[j]
            if temp1[0:(s-2)] == temp2[0:(s-2)]:
                combo.append(list(set(temp1) | set(temp2)))
            else:
                break
    return combo


# In[9]:


def aprioriAlgo(basket, support, numberOfBaskets):
    basket = list(basket)
    oneItem = Counter()
    partitionSupport = float(support*(float(len(basket))/float(numberOfBaskets)))
    print(partitionSupport)
    result = list()
#     s = 1
    for item in basket:
        oneItem.update(item)

    candidateSingleItems = {x : oneItem[x] for x in oneItem if oneItem[x] >= partitionSupport }
#     candidateItems = {}
#     print(len(candidateItems))
#     for item in oneItem:
#         if oneItem[item]>= partitionSupport:
#             candidateItems[item] = oneItem[item]

    frequentSingleItem = sorted(candidateSingleItems)
#     print(frequentItem)
    result.extend(frequentSingleItem)
#     candidateItems.clear()
#     print(candidateItems)
#     print(result)
#     print(type(candidateItems))
#     print(candidateItems)
#     print(partitionSupport)
#     s = 2
    frequentItem = set(frequentSingleItem)

#     print(combinations(frequentItem,2))
    pairsOfTwo = list()
    for item in combinations(frequentItem,2):
        temp = list(item)
        temp.sort()
        pairsOfTwo.append(temp)
#     print(pairsOfTwo)
    candidateItems = pairsOfTwo
#     print(candidateItems)
    updatedFrequentItem = frequentItemsGenerator(basket,candidateItems,partitionSupport)
    result.extend(updatedFrequentItem)
#     print(updatedFrequentItem)
    frequentItem = list(set(updatedFrequentItem))
    frequentItem.sort()
    s = 3

    while len(frequentItem)!= 0:
        candidateItems = candidateItemsGenerator(frequentItem,s)
        updatedFrequentItem = frequentItemsGenerator(basket,candidateItems,partitionSupport)
        result.extend(updatedFrequentItem)
        frequentItem = list(set(updatedFrequentItem))
        frequentItem.sort()
        s = s+1

#     print(result)
    return result


# In[10]:


def candidateCount(basket, candidateSet):
    countDictionary = {}
    basket = list(basket)

    for candidate in candidateSet:
        if type(candidate) is str:
            candidate = [candidate]
            key = tuple(sorted(candidate))
        else:
            key = candidate

        candidate = set(candidate)
        for item in basket:
            if candidate.issubset(item):
                if key in countDictionary:
                    countDictionary[key] = countDictionary[key] + 1
                else:
                    countDictionary[key] = 1

    return countDictionary.items()
#27436


# In[11]:


phase1Map = onlyBaskets_rdd.mapPartitions(lambda x: aprioriAlgo(x,support,numberOfBaskets)).map(lambda x : (x, 1))
# print(phase1Map.collect())
phase1Reduce = phase1Map.reduceByKey(lambda x,y: (1)).keys().collect()
# print(phase1Reduce)
end = time.time()
# print("Time taken: ", end - start, " seconds")
# phase1Reduce


# In[12]:


# for x in onlyBaskets_rdd.collect():
#     print(x)
#Phase 2
phase2Map = onlyBaskets_rdd.mapPartitions(lambda baskets : candidateCount(baskets, phase1Reduce))
# print(mapOutput2.collect())
phase2Reduce = phase2Map.reduceByKey(lambda x,y: (x+y))
# print(phase2Reduce.collect())

final = phase2Reduce.filter(lambda x: x[1] >= support)
end = time.time()
# print("Time taken: ", end - start, " seconds")


# In[13]:


#  final.collect()
print(phase2Reduce.collect())


# In[14]:


final.collect()


# In[15]:


tem = final.map(lambda x: x[0]).collect()
tem


# In[16]:


tem.sort(key=lambda t: (len(t),t))
len(tem)
tem
# a = sorted(tem, key = lambda item: (len(item), item))


# In[17]:


# tem[3015]


# In[18]:


sizeOfWrite = len(tem)


# In[19]:


tempList = list()
i=0
j=0
previousLength = len(tem[0])
currentLength =  len(tem[1])
fileName = sys.argv[3]
f = open(fileName, "w")
while i < sizeOfWrite:
    while((previousLength == currentLength) and i+1<sizeOfWrite):
        previousLength = len(tem[i])
        currentLength = len(tem[i+1])
        tempList.append(tem[i])
#         print(tem[i])
        j = j+1
        i = i + 1
    print(i)
    if i+1 == sizeOfWrite:
        tempList.append(tem[sizeOfWrite-1])
#     print(tempList)
    if (len(tempList[0])==1):
        a = ",".join([str(s).replace('\'','').replace(',','') for s in tempList])
        print(a)
    else:
        a = ",".join([str(s).replace('\'','').replace(' ','') for s in tempList])
        print(a)

#     print(a)
    f.write(a)
    f.write("\n\n")
    j = 0
    del tempList[:]
    previousLength = 0
    currentLength = 0
    if(i == sizeOfWrite-1):
        f.close()
        break

end = time.time()
print("Time taken: ", end - start, " seconds")


# In[20]:


# ",".join([str(s).replace('\'','') for s in tem])


# In[21]:


# 40 - 15.8644309044  seconds
# small 500 - 29.175962925  seconds
# small 1000 - 7.65758991241  seconds
