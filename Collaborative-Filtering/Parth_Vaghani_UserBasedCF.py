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


time1 = time.time()


# In[2]:


#/Users/parth/Downloads/hw2/Data/train_review.csv
trainCSV = sys.argv[1]
testCSV = sys.argv[2]
# trainCSV = "/Users/parth/Downloads/hw2/Data/train_review.csv"
# testCSV = "/Users/parth/Downloads/hw2/Data/test_review.csv"
# trainCSV = "/Users/parth/Downloads/train_review1.csv"
# testCSV = "/Users/parth/Downloads/test_review1.csv"


# In[3]:


sc = SparkContext(appName="task2")


# In[4]:


trainData = sc.textFile(trainCSV,None,False)
trainDataHeader = trainData.first()
trainDataNoHeader = trainData.filter(lambda x: x != trainDataHeader) 

testData = sc.textFile(testCSV,None,False)
testDataHeader = testData.first()
testDataNoHeader = testData.filter(lambda x: x != trainDataHeader) 
testDataNoHeader.collect()


# In[5]:


rating_rdd = trainDataNoHeader.map(lambda x: x.split(',')).map(lambda x: ((x[0]),((x[1]),float(x[2])))).groupByKey().sortByKey(True) 
d_user_rating_rdd = rating_rdd.mapValues(dict).collectAsMap() #user as key
#len(d_user_rating_rdd)


# In[6]:


rating_rdd_b = trainDataNoHeader.map(lambda x: x.split(',')).map(lambda x: ((x[1]),((x[0]),float(x[2])))).groupByKey().sortByKey(True) 
d_business_rating_rdd = rating_rdd_b.mapValues(dict).collectAsMap() #Business as key
#len(d_business_rating_rdd)


# In[7]:


#broadcast user and business dictionaries

bc_d_user_rating_rdd = sc.broadcast(d_user_rating_rdd)
bc_d_business_rating_rdd = sc.broadcast(d_business_rating_rdd)

#len(bc_d_business_rating_rdd.value)


# In[8]:


# f.write("UserID,MovieId,Pred_rating" + "\n") 
# def weightCalculation(userA, businessesA, userB, businessesB, business):
# #     print(userA + " " + " " + userB + " " )
#     businessesA.sort(key=itemgetter(0))
#     businessesB.sort(key=itemgetter(0))
# #     for x in businessesB:
# #         print x[0], x[1]
#     userAIndex = 0
#     userBIndex = 0
#     totalRatingsA = 0 #total Ratings for average numerator
#     totalRatingsB = 0 #total Rating for average Numerator
#     commonBusinessIndexA = [] #index at which there are common business
#     commonBusinessIndexB = [] #index at which there are common business
#     calculatedWeight = 0 #calculated pearson weight between user A and User B
#     containsBusiness = False #conatins business name to be predicted
#     valueContained = 0 #value contained at business to be predicted
    
#     while((userAIndex < len(businessesA)) and userBIndex < len(businessesB)):
#         if(business < businessesB[userBIndex][0] and (containsBusiness == False)):
#             return ((userA,userB),-1,0)
#         if(businessesA[userAIndex][0] == businessesB[userBIndex][0]):
#             #add to common index list and sum ratings
#             commonBusinessIndexA.append(userAIndex)
#             totalRatingsA = totalRatingsA + businessesA[userAIndex][1]
            
#             commonBusinessIndexB.append(userBIndex)
#             totalRatingsB = totalRatingsB + businessesB[userBIndex][1]
            
#             userAIndex = userAIndex + 1
#             userBIndex = userBIndex + 1
            
#         elif(businessesA[userAIndex][0] > businessesB[userBIndex][0]):
#             if (businessesB[userBIndex][0] == business):
#                 containsBusiness = True
#                 valueContained = businessesB[userBIndex][1]
#             userBIndex = userBIndex + 1
            
#         elif(businessesA[userAIndex][0] < businessesB[userBIndex][0]):
#             if (businessesB[userBIndex][0] == business):
#                 containsBusiness = True
#                 valueContained = businessesB[userBIndex][1]
#             userAIndex = userAIndex + 1
            
    
# #     print(containsBusiness)
# #     print(valueContained)
#     if containsBusiness == False or (len(commonBusinessIndexA) == 0 or len(commonBusinessIndexB) == 0):
#         return ((userA,userB),-1,0)
    
#     averageUserA = totalRatingsA / len(commonBusinessIndexA) #average of user A
#     averageUserB = totalRatingsB / len(commonBusinessIndexB) # average of user B
    
# #    print("aversge A -" , averageUserA , "average B-", averageUserB)
    
#     numerator = 0
#     denominatorAsq = 0 #sqare of A
#     denominatorBsq = 0 #sqare of B
    
#     minlength = 0
    
#     if (len(commonBusinessIndexA) > len(commonBusinessIndexB)):
#         minlength = len(commonBusinessIndexB)
#     else:
#         minlength = len(commonBusinessIndexA)
    
#     for index in range(0, minlength):
#         ba = businessesA[commonBusinessIndexA[index]][1] - averageUserA
#         bb = businessesB[commonBusinessIndexB[index]][1] - averageUserB
#         numerator += (ba) * (bb)
#         denominatorAsq += pow(ba,2)
#         denominatorBsq += pow(bb,2)
#         #print("asq - " , ba , ' bb-', bb)
        
#     denominator = math.sqrt(denominatorAsq) * math.sqrt(denominatorBsq)
    
#     #print("numerator - ", numerator, " denominator -", denominator)
    
#     if(denominator != 0):
#         calculatedWeight = numerator / denominator
    
# #     print(calculatedWeight)
    
#     predictionDifferenceWeight = (valueContained - averageUserB) * calculatedWeight
    
# #     for a in businessesA:  # <-- this unpacks the tuple like a, b = (0, 1)
# #         print(a)
    
#     return ((userA,userB),predictionDifferenceWeight,calculatedWeight)
    

###################################################################################################################
# for line in target_res_rdd: #iterate through each user for which prediction has to be made
#     otherThanUserData = rating_rdd.filter(lambda x: x[0] != line[0]) #traingData
#     userData = rating_rdd.filter(lambda x: x[0] == line[0]) #current user data
#     userData = userData.collect()
#     #print(userData)
#     #print(otherThanUserData.collect()) # Names other than user
#     #print(line)
#     if(len(list(userData))==0):
#         print("OK")
#         continue
#     userADatalist = list(userData[0][1]) #contains userA ratingd for average
#     userARatingAverage = 0 
#     userARatingSum = 0 
#     for i in range(0,len(userADatalist)):
#         userARatingSum += userADatalist[i][1]
#     userARatingAverage = userARatingSum / len(userADatalist)
#     #print(userARatingAverage)
    
#     for business in line[1]:#iterate through each business
#         predictedWeight = otherThanUserData.map(lambda x: weightCalculation(line[0],list(userData[0][1]),x[0],list(x[1]),business)).filter(lambda x: x[2] != 0)
#         #print(predWeight.collect())
#         #predictedWeightResult = predictedWeight.collect()
#         #print(predictedWeight.collect())
#         #print(predictedWeightResult)
        
#         predictionNumerator = predictedWeight.map(lambda x: (1,x[1])).reduceByKey(add).collect()
#         #print(predictionNumerator)
#         predictionDenominator = predictedWeight.map(lambda x: (1,abs(x[2]))).reduceByKey(add).collect()
#         #print(predictionDenominator)
        
#         if(len(predictionNumerator) == 0 or len(predictionDenominator) == 0):
#             prediction  = userARatingAverage
#             f.write(str(line[0]) + "," + str(business) + "," + str(prediction) +"\n")
#             continue
        
#         if(predictionDenominator[0][1] == 0):
#             prediction = userARatingAverage
#         else:
#             prediction = userARatingAverage + (predictionNumerator[0][1] / predictionDenominator[0][1])
        
#         f.write(str(line[0]) + "," + str(business) + "," + str(prediction) +"\n")
        
#         print(prediction)
# f.close()

# 


def weightCalculation(userA,business,bc_d_business_rating_rdd,bc_d_user_rating_rdd):
    business_rating_rdd = bc_d_business_rating_rdd.value
    user_rating_rdd = bc_d_user_rating_rdd.value
    if(userA in user_rating_rdd): #check for new user
        userAIndex = 0 #userA businesses Index
        totalRatingsA = 0 #total Ratings for average numerator
        totalRatingsB = 0 #total Rating for average Numerator
        calculatedWeight = 0 #calculated pearson weight between user A and User B
        valueContained = 0 #value contained at business to be predicted
        userABusinessList = list(user_rating_rdd.get(userA))
        #print(userABusinessList)
        userABusiness = user_rating_rdd.get(userA)
        userARatingSum = sum(userABusiness.itervalues())
        #print(userARatingSum)
        weightRateList = [] #tuple of difference and weight
        commonBusinessIndexA = [] #index at which there are common business
        commonBusinessIndexB = [] #index at which there are common business
        userAAverage = userARatingSum / len(userABusiness)
#         print(business_rating_rdd.get(business))
        if (business_rating_rdd.get(business) == None): #new business
            #print(userAverage)
            return (userA, business,str(userAAverage))
        else:
            businessUsersList = list(business_rating_rdd.get(business))#list of users who rated business
#             print(businessUsersList)
            if(len(businessUsersList) != 0): #old user
                for i in range(0, len(businessUsersList)):
                    totalRatingsA = 0 #total Ratings for average numerator
                    totalRatingsB = 0 #total Rating for average Numerator
                    userAIndex = 0 #userA businesses Index
                    del commonBusinessIndexA[:]
                    del commonBusinessIndexB[:]
                    current_business_value = user_rating_rdd[businessUsersList[i]].get(business) #rating of business by this user
                    while(userAIndex < len(userABusinessList)):
#                         print("\n userrdd rating - ",user_rating_rdd[businessUsersList[i]].get(userABusinessList[userAIndex]) )
                        if(user_rating_rdd[businessUsersList[i]].get(userABusinessList[userAIndex])):
                            totalRatingsA += user_rating_rdd[userA].get(userABusinessList[userAIndex])
                            totalRatingsB += user_rating_rdd[businessUsersList[i]].get(userABusinessList[userAIndex])
                            commonBusinessIndexA.append(user_rating_rdd[userA].get(userABusinessList[userAIndex]))
                            commonBusinessIndexB.append(user_rating_rdd[businessUsersList[i]].get(userABusinessList[userAIndex]))
                        userAIndex += 1
                    userAIndex = 0
                    if(len(commonBusinessIndexA) != 0):
                        averageUserA = totalRatingsA / len(commonBusinessIndexA) #average of user A
                        averageUserB = totalRatingsB / len(commonBusinessIndexB) # average of user B
                        #    print("aversge A -" , averageUserA , "average B-", averageUserB)
                        numerator = 0
                        denominatorAsq = 0 #sqare of A
                        denominatorBsq = 0 #sqare of B
                        for i in range(0,len(commonBusinessIndexA)):
                            ba = commonBusinessIndexA[i] - averageUserA
                            bb = commonBusinessIndexB[i] - averageUserB
                            numerator += (ba) * (bb)
                            denominatorAsq += pow(ba,2)
                            denominatorBsq += pow(bb,2)
                            #print("asq - " , ba , ' bb-', bb)
                            
                        denominator = math.sqrt(denominatorAsq) * math.sqrt(denominatorBsq)
                        if(denominator != 0):
                            calculatedWeight = numerator / denominator
#                             print(calculatedWeight)
                        predictionDifferenceWeight = (current_business_value - averageUserB) * calculatedWeight
                        weightRateList.append((predictionDifferenceWeight,calculatedWeight))
                #print(weightRateList)
                predictionNumerator = 0
                predictionDenominator = 0
                for i in range (0,len(weightRateList)):
                    predictionNumerator += weightRateList[i][0]
                    predictionDenominator += abs(weightRateList[i][1])
#                 print(predictionNumerator)
#                 print(predictionDenominator)
                prediction = -1
                if(predictionNumerator == 0 or predictionDenominator == 0):
                    prediction = userAAverage
                    return (userA,business,str(prediction))
                else:
                    prediction = userAAverage + (predictionNumerator / predictionDenominator)
                    if(prediction<0):
                        prediction = 0.0
                    elif (prediction>5):
                        prediction = 5.0
                    return (userA,business,str(userAAverage)) #prediction
            else: #new user
                return (userA, business, "2.7")
                
    else:
        return (userA,business,str("2.7"))
                
        


# In[9]:


testData_rdd = testDataNoHeader.map(lambda x: x.split(",")).sortBy(lambda x:((x[0]),(x[1])))

weightc = testData_rdd.map(lambda x: weightCalculation(x[0],x[1],bc_d_business_rating_rdd, bc_d_user_rating_rdd))
tempweight = weightc.collect()

# weightCalculation("wtUwFJFfFBJ4FR59QcptHA","K7lWdNUhCbcnEvI0NhGewg",bc_d_business_rating_rdd, bc_d_user_rating_rdd)


# In[10]:


fileName = "Parth_Vaghani_UserBased.txt"
f = open(fileName, 'w')
# f.write("UserID,MovieId,Pred_rating" + "\n") 

for i in range(0,len(tempweight)):
    f.write(str(tempweight[i][0]) + "," + str(tempweight[i][1]) + "," + str(tempweight[i][2]) +"\n")

f.close()


# In[11]:


fread = sc.textFile(fileName,None,False)
fheader = fread.first()
fdataNoHeader = fread.filter(lambda x: x != fheader).map(lambda x: x.split(','))
#print(fdata.collect())
#fSplitValues = weightc.map(lambda x: (((x[0]),(x[1])),float(x[2])))
fSplitValues = fdataNoHeader.map(lambda x: (((x[0]),(x[1])),float(x[2])))
testDataSplitValues = testDataNoHeader.map(lambda x: x.split(',')).map(lambda x: (((x[0]),(x[1])),float(x[2])))
checkTestData = testDataSplitValues.join(fSplitValues).map(lambda x: (((x[0]),(x[1])),abs(x[1][0]-x[1][1])))
tempCheckData = checkTestData.collect()
# print(fSplitValues.collect())
# print(testDataSplitValues.collect())
# print(joinTestData.collect())
# a = joinTestData.map(lambda x: (x[1])).collect()
# print(a)
# print(tempCheckData[1])
g0l1 = 0
g1l2 = 0
g2l3 = 0
g3l4 = 0
g4 = 0
for i in range(0,len(tempCheckData)):
    if(tempCheckData[i][1] >=0 and tempCheckData[i][1] < 1):
        g0l1 += 1
    elif (tempCheckData[i][1] >=1 and tempCheckData[i][1] < 2):
        g1l2 += 1
    elif (tempCheckData[i][1] >=2 and tempCheckData[i][1] < 3):
        g2l3 += 1
    elif (tempCheckData[i][1] >=3 and tempCheckData[i][1] < 4):
        g3l4 += 1
    elif (tempCheckData[i][1] >=4):
        g4 += 1


# for i in range(0, len)
print(">=0 and <1:", g0l1)
print(">=1 and <2:",g1l2)
print(">=2 and <3:",g2l3)
print(">=3 and <4:",g3l4)
print(">=",g4)
rdd1=checkTestData.map(lambda x:x[1]**2).reduce(lambda x,y:x+y)
rmse=math.sqrt(rdd1/fSplitValues.count())
# print("RDD: "rdd1)
print("RMSE: ",rmse)
time2 = time.time()
print("Time: ",time2-time1)


# In[12]:


# >=0 and <1: 28825
# >=1 and <2: 13163
# >=2 and <3: 2730
# >=3 and <4: 473
# >= 44
# RMSE:  1.08786646922
# Time:  122.787189007
    
    
    
#     >=0 and <1:28693
# >=1 and <2:13094
# >=2 and <3:2337
# >=3 and <4:301
# >=4 :35
# RMSE = 1.0789417495445968
# Time :34sec

