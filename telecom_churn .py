#!/usr/bin/env python
# coding: utf-8

# # Telecom Churn
# - Team 1
# - Team members
#             Tashi Chotso - 20BDA01
#             Jerry Raju Mathew - 20BDA16
#             Gunam Ramya Sri - 20BDA33
#             PrajjWal Patel - 20BDA48

# ## Problem statement
# - In the telecom industry, customers are able to choose from multiple service providers and actively switch from one operator to another. In this highly competitive market, the telecommunications industry experiences an average of 15-25% annual churn rate. Given the fact that it costs 5-10 times more to acquire a new customer than to retain an existing one, customer retention has now become even more important than customer acquisition or reduce customer churn, telecom companies need to predict which customers are at high risk of churn.
# 
# 1. Extract, load, and read the data as text files as RDD Transform    
# 2. Transform: Exploratory data analysis using rdd
# 
#     - A. Replace Contract column values
#         - Month-to-month -1m
#         - One Year - 1y
#         - Two Year - 2y
#         - rest all - Others
#         
#     - B. Unique customer count
# 
#     - C. Describe the categorical and numerical columns seperately
# 
#     - D. GroupBy contract and avg of totalcharges
# 
#     - E. Using accumulator add the totalcharges                         
#     
#          
# 3. Load: Save analysis report
# 
#     - GroupBy contract and avg of totalcharges save as files

# In[81]:


import pyspark
import os
from random import random
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import numpy as np
from pyspark.mllib.stat import Statistics


# The first thing a Spark program must do is to create a SparkContext object, which tells Spark how to access a cluster. To create a SparkContext you first need to build a SparkConf object that contains information about your application.

# ## 1. Extract, load, and read the data as text files as RDD Transform

# In[82]:


spark = SparkSession.builder.master("local").        appName("SparkApplication").        config("spark.driver.bindAddress","localhost").        config("spark.ui.port","4041").        getOrCreate()


# In[83]:


spark


# ### To read CSV  in Spark into single RDD.

# In[84]:


sc = spark.sparkContext


# In[85]:


#2.read csv file in rdd
data=sc.textFile("telecomChurn.csv")


# In[86]:


print('\n',data) #what file rdd


# In[87]:


print('\n',type(data)) # variable type


# In[88]:


print('\n',dir(data))# what attributes are avaiable 


# In[89]:


#header
header=data.first()
print(header)


# In[90]:


# remove header
rdd1= data.filter(lambda line: line !=header)


# In[91]:


#total record counts
print('\n file has:',rdd1.count(),'row') #counts


# In[92]:


#filter first row
print('\n file line:',rdd1.first())


# In[93]:


rdd1.take(5) #take file element


# In[94]:


#total unique records count
rdd1.distinct().count()


# In[95]:


print("initial partition count:"+str(rdd1.getNumPartitions()))
#Outputs: initial partition count:2


# In[96]:


step1= rdd1.map(lambda line: line.split(",")) # split by ,


# In[97]:


step1.take(2)


# ## 2. Transform: Exploratory data analysis using rdd
# 
# ##   A. Replace Contract column values
# 
#     - Month-to-month -1m
#     - One Year - 1y
#     - Two Year - 2y
#     - rest all - Others

# In[98]:


#replaced column value
def replace(column_val):
    if column_val=="Month-to-month":
        column_val="1m"
    elif column_val=="One year":
        column_val="1y"
    elif column_val=="Two year":
        column_val="2y"
    else:
        column_val= "Others"
    return column_val


# In[99]:


# Contract column values passed to the replace function and assigned new column value
step2=step1.map(lambda x: (x[0],x[1],x[2],x[3],x[4],x[5],
                           x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],replace(x[15]),x[16],x[17],x[18],x[19],x[20]))


# In[100]:


#show first two records 
step2.take(2)


# In[101]:


## function to convert numerical columns from string to int
def string_to_int(val):
    try:
        return int(float(val))
    except:
        return 0


# In[102]:


#Column 2,5,18,19 are numberical column so apply string_to_int function for converting string to int
step3=step2.map(lambda x: (x[0],x[1],string_to_int(x[2]),x[3],x[4],string_to_int(x[5]),
                           x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15],x[16],x[17],string_to_int(x[18]),string_to_int(x[19]),x[20]))


# In[103]:


# show first record
step3.take(1)


# ## C. Describe the categorical and numerical columns seperately

# ### i. Numerical data

# In[104]:


#show only numerical columns in the given dataset
num_data=step3.map(lambda x: (x[2],x[5],x[18],x[19]))


# In[105]:


#show first four records
num_data.take(4)


# #### mean and variance of numerical data

# In[145]:


#import necessary packages 
import numpy as np
from pyspark.mllib.stat import Statistics
num_data=step3.map(lambda x: (x[2],x[5],x[18],x[19])) # numberical cols 
num_data.take(5)
summary = Statistics.colStats(num_data)
print("------ statistic analysis -----------")
print("mean :",summary.mean())  # a dense vector containing the mean value for each column
print("variance :",summary.variance())  # column-wise variance


# #### Maximum and minimum entry of numerical data

# In[146]:


#aggregate function for numerical columns
print("------statistic analysis----------------------")
print(" Maximum  entry  in each column",num_data.max())
print("Minimum  entery  in each column",num_data.min())


# ### ii. Categorical data

# In[108]:


#show categorical columns 
cat_data=step3.map(lambda x:(x[0],x[1],x[3],x[4],x[6],[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15],x[16],x[17],x[20]))
#show first records of category data
cat_data.take(1)


# In[109]:


#Number of records 
cat_data.count() 


# In[110]:


##User defined functions 
# Module for changing string to int
def string_to_int(val):
    try:
        return int(float(val))
    except:
        return 0

# Module getting the count
def count(x):
    temp=0
    for val in list(x):
        temp = temp + val[1]
        out = str(val[0])+","+str(temp)
    return (out)

#module for average
def mean_val(x):
    sums=0
    l=0
    for i in x:
        sums= sums + i[1]
        l=l+1
        avg=round(sums/l,2)
        
    return (avg)

## module for replace value
def replace(column_val):
    if column_val=="Month-to-month":
        column_val="1m"
    elif column_val=="One year":
        column_val="1y"
    elif column_val=="Two year":
        column_val="2y"
    else:
        column_val= "Others"
    return column_val


# ### count Churn
# - Counting number of customer  churn "Yes"  or not churn "No"

# In[144]:


# excluding header, split data by ',', count number of customer churn "Yes" or not churn "No"
col=[0,1,3,4,6,7,8,9,10,11,12,13,14,15,16,17,20]
cat_count = data.filter(lambda line: line != header).            map(lambda line: line.split(",")).            map(lambda x: (x[0],x[1],x[2],x[3],x[4],x[5],
                           x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],replace(x[15]),x[16],x[17],x[18],x[19],x[20])).map(
    lambda x: ((x[20]), string_to_int(x[5]))).\
    groupBy(lambda x: x[0]).\
    map(lambda x: (count(x[1]))).coalesce(1)
print("----count number of customer churn or not churn ------------")
cat_count.collect()


# In[143]:


#Count number of gender "Female" and 'Male'
cat_ge = data.filter(lambda line: line != header).            map(lambda line: line.split(",")).           map(
    lambda x: ((x[1]), string_to_int(x[5]))).\
    groupBy(lambda x: x[0]).\
    map(lambda x: (count(x[1]))).coalesce(1)
print("----------Count number of famale and male-------- ")
print(cat_ge.collect())


# ### Distribution of different internet plans 

# In[141]:


#Count number of mobile services such as DSL,Fiber and No
cat_service = data.filter(lambda line: line != header).            map(lambda line: line.split(",")).map(
    lambda x: ((x[8]), string_to_int(x[5]))).\
    groupBy(lambda x: x[0]).\
    map(lambda x: (count(x[1]))).coalesce(1)
print("-------shown different internet plans----------")
print(cat_service.collect())


# ###  Distribution of different PaymentMethod

# In[139]:


#counting number of payment method such as electronic check, mailed check, bang transfer, and credit card
cat_pyMethod = data.filter(lambda line: line != header).            map(lambda line: line.split(",")).map(
    lambda x: ((x[17]), string_to_int(x[5]))).\
    groupBy(lambda x: x[0]).\
    map(lambda x: (count(x[1]))).coalesce(1)
print("-----shown different Payment Method-----------")
print(cat_pyMethod.collect())


# ### Distribution of different service Lines

# In[137]:


#counting number of serivice lines such as "no phone service",'No','Yes'
cat_pyMethod = data.filter(lambda line: line != header).            map(lambda line: line.split(",")).map(
    lambda x: ((x[7]), string_to_int(x[5]))).\
    groupBy(lambda x: x[0]).\
    map(lambda x: (count(x[1]))).coalesce(1)
print("----------Different service Lines-------- ")
cat_pyMethod.collect()


# ### Number of  customer churn w.r.t gender

# In[131]:


# couting number of churn or not churn with respect to the gender
cat_gender = data.filter(lambda line: line != header).            map(lambda line: line.split(",")).map(
    lambda x: ((x[20],x[1]), string_to_int(x[5]))).\
    groupBy(lambda x: x[0]).\
    map(lambda x: (count(x[1]))).coalesce(1)

print("---Number of customer churn and not churn w.r.t gender----")
print(cat_gender.collect())


# ## B. Unique customer count

# In[133]:


cat_customer = data.filter(lambda line: line != header).            map(lambda line: line.split(",")).map(
    lambda x: ((x[0]), string_to_int(x[5]))).\
    groupBy(lambda x: x[0]).\
    map(lambda x: (count(x[1]))).coalesce(1)

print("Unique Customer")
print("show first three unique customer",cat_customer.take(3))
print("----------------------------------------------------------------------")
print("Number of unique customer",cat_customer.distinct().count())


# ## D.  GroupBy contract and avg of totalcharges 

# In[76]:


#show contract and totalcharge column
step4=step3.map(lambda x: (x[15],x[19]))
step4.take(2)


# In[134]:


#groupby contract and avg totalcharges   
def mean_val(x):
   sums=0
   l=0
   for i in x:
       sums= sums + i[1]
       l=l+1
       avg=round(sums/l,2)
       
   return (avg)
   
step5= step4.map(lambda x:((x[0]),string_to_int(x[1])))           .map(lambda x: (x[0],x[1]))           .groupBy(lambda x: (x[0])).           map(lambda x: (x[0],mean_val(x[1]))).coalesce(1)

print("---------Average totalcharges in each of 1m,1y and 2y--------------")           
step5.collect() 


# #### As we can see above analysis,  average total charge in 1m is ->1368.79, 1y->3032.14 and 2y->3706.47

# ## E. Using accumulator add the totalcharges

# In[136]:


#starting from 0 and add up all the totalcharges 
accuSum=spark.sparkContext.accumulator(0)
def countFun(x):
    global accuSum
    accuSum+=x
totalcharges.foreach(countFun)
print("-----Total Charges-------------------")
print("Total Charges occured: ", accuSum.value)
print("---------------------------------------")

accumCount=spark.sparkContext.accumulator(0)
totalcharges.foreach(lambda x:accumCount.add(1))
print("Number of customer entries: ", accumCount.value)


# # 3. Load: Save analysis report
# 
# - GroupBy contract and avg of totalcharges save as files

# In[42]:


# save textfile in folder Report with file name avg_totalcharges
step5.saveAsTextFile("Report/avg_totalCharges")


# --------------------------------------------------------END-------------------------------------------------------------------
