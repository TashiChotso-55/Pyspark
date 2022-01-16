# Pyspark
Telecom churn Analysis using RDD 
In the telecom industry, customers are able to choose from multiple service providers and actively switch from one operator to another. In this highly competitive market, the telecommunications industry experiences an average of 15-25% annual churn rate. Given the fact that it costs 5-10 times more to acquire a new customer than to retain an existing one, customer retention has now become even more important than customer acquisition.o reduce customer churn, telecom companies need to predict which customers are at high risk of churn.
1. Telecom - churn data set

i.Extract:  Load the data

    - Read data as text file as RDD

ii.Transform: Exploratory data analysis using rdd

    - replace Contract column values

        - Month-to-month -1m

        - One Year - 1y

        - Two Year - 2y

        - rest all - Others

    - Unique customer count

    - describe the categorical and numerical columns seperately

    - GroupBy contract and avg of totalcharges

    - using accumulator add the totalcharges

iii.Load: Save analysis report

    - GroupBy contract and avg of totalcharges save as files


