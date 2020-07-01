#!/usr/bin/env python
# coding: utf-8

# Spark Assignment #3 : Create Application
# 
# Author: Tryambak Kaushik

# In[1]:


import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import findspark
import datetime
findspark.init()

from pyspark import SparkContext, SparkConf, SQLContext, HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import DateType, IntegerType
import argparse


# In[2]:


MYDIRS3='/user/hdfs/sparkdata/soln3'


# In[3]:


#get_ipython().run_cell_magic(u'bash', u'-s "$MYDIRS3"', u'\nif hdfs dfs -test -e $1 ; then \necho "Deleting existing $1 on HDFS"\nhadoop fs - rm -R $1\nfi\n\necho "Creating new $1 on HDFS"\nhadoop fs -mkdir $1')


# In[4]:


job_date='20200608'
MYDIR='/user/hdfs/sparkdata'
hdfs_orders_path="hdfs://quickstart.cloudera:8020"+MYDIR+"/orders.csv"
hdfs_orders_details_path="hdfs://quickstart.cloudera:8020"+MYDIR+"/orderdetails.csv"
hdfs_customers_path="hdfs://quickstart.cloudera:8020"+MYDIR+"/customers.csv"
hive_db='orders'
hive_table='final_list_hv'


# In[5]:


# %%bash -s "$MYDIR"

# if ! hdfs dfs -test -e $1 ; then 
# echo "Creating $1 on HDFS"
# hadoop fs -mkdir $1
# fi

# hadoop fs -find / -name $1

# echo "Transfering files from local file system to hadoop file system"
# for file in /var/lib/hadoop-hdfs/datauniversity/spark/orders/*.csv
# do
#  hadoop fs -put $file $1
# done

# hadoop fs -ls $1


# In[6]:


def read_orders(path):
    orders=sc.textFile(path)
    ordersrdd=orders.map(lambda line: (line.split('|')[0].strip('"'), 
                                       line.split('|')[1].strip('"'),
                                       line.split('|')[2].strip('"'),
                                       line.split('|')[3].strip('"'), 
                                       line.split('|')[4].strip('"'),
                                       line.split('|')[5].strip('"'),
                                       line.split('|')[6].strip('"')))  
    df_orders=hiveContext.createDataFrame(ordersrdd, schema=['orderNumber', 'orderDate', 
                                                            'requiredDate', 'shippedDate', 
                                                            'status', 'comments', 
                                                            'customerNumber'])     
    df_orders.registerTempTable('myorders')
    return df_orders


def read_orders_details(path):
    orderdetails=sc.textFile(path)
    orderdetailsrdd=orderdetails.map(lambda line: (line.split('|')[0].strip('"'), 
                                                   line.split('|')[1].strip('"'),
                                                   line.split('|')[2].strip('"'), 
                                                   line.split('|')[3].strip('"'),
                                                   line.split('|')[4].strip('"'))).collect()
    df_orderdetails=hiveContext.createDataFrame(orderdetailsrdd, 
                                               schema=['orderNumber', 'productCode', 
                                                       'quantityOrdered', 'priceEach', 
                                                       'orderLineNumber'])
    df_orderdetails.registerTempTable('myorderdetails')
    return df_orderdetails


def read_customers(path):
    customers=sc.textFile(path)
    customersrdd=customers.map(lambda line: (line.split('|')[0].strip('"'), 
                                       line.split('|')[1].strip('"'),
                                       line.split('|')[2].strip('"'),
                                       line.split('|')[3].strip('"'), 
                                       line.split('|')[4].strip('"'),
                                       line.split('|')[5].strip('"'),
                                       line.split('|')[6].strip('"'),
                                       line.split('|')[7].strip('"'),
                                       line.split('|')[8].strip('"'),
                                       line.split('|')[9].strip('"'), 
                                       line.split('|')[10].strip('"'),
                                       line.split('|')[11].strip('"'),
                                       line.split('|')[12].strip('"')))
    df_customers=hiveContext.createDataFrame(customersrdd, schema=['customerNumber', 'customerName',          
                                                                        'contactLastName', 'contactFirstName',      
                                                                        'phone', 'addressLine1',          
                                                                        'addressLine2', 'city',                  
                                                                        'state', 'postalCode',            
                                                                        'country', 'salesRepEmployeeNumber',
                                                                        'creditLimit' ])
    df_customers.registerTempTable('mycustomers')
    return df_customers


# In[7]:


# 1. Create a list of customers (include demographic information) 
# whose order were late (point 1 above). This list should have
# an additional column called Gift Card Amount. Populate Gift
# Card Amount as follows: If total order exceeds $100 then
# Gift Card Amount=$50 else Gift Card Amount=$25

def soln_Assignment3Q1():
    print("""Q1: Create a list of customers (include demographic information) 
    whose order were late (point 1 above). This list should have an
    additional column called Gift Card Amount. Populate Gift Card
    Amount as follows: If total order exceeds $100 then Gift Card
    Amount=$50 else Gift Card Amount=$25""")

    #Solution
    # Read file for HDFS
    df_orders=read_orders(hdfs_orders_path)
    df_orderdetails=read_orders_details(hdfs_orders_details_path)
    df_customers=read_customers(hdfs_customers_path)

    df_temp1=df_orders.join(df_orderdetails, on='orderNumber').filter(df_orders.requiredDate < df_orders.shippedDate)
    df_temp2=df_customers.join(df_temp1, on='customerNumber')

    print("\n")
    print("""A1.1: The list of customers whose 'total order amount' (quantityOrdered * priceEach)
    of one product is greater than 100 and delayed""")

    df_temp=df_temp2.withColumn('Total',
        round(df_temp2.quantityOrdered * df_temp2.priceEach, 2)).filter(col('Total')>100).sort('Total')
    df_final=df_temp.withColumn('GiftCardAmount', when(col('Total')>100, 50).otherwise(25))
    df_final.show(5)
    print "The total number of customers for the above 'total order amount' based list = ", df_final.count()

    print("\n")
    print("""A1.2: The list of customers whose 'priceEach' amount
    of one product is greater than 100 and delayed""")
    df_temp=df_temp2.filter(col('priceEach')>100).sort('priceEach')
    df_final=df_temp.withColumn('GiftCardAmount', when(col('priceEach')>100, 50).otherwise(25))
    df_final.show()
    print "The total number of customers for the above 'priceEach' based list = ", df_final.count()
    return df_final


# In[8]:


#2. Create a HIVE table for the list above

def soln_Assignment3Q2(df_final):
    print("\n")
    print("Q2: Create a HIVE table from the list above")

    print("\nA2: The HIVE table is (showing only 2 rows): ")
    #Register the pyspark dataframe in temptable
    hiveContext.sql("Drop table if exists myfinaltemphv")
    df_final.registerTempTable("myfinaltemphv")

    hiveContext.sql("CREATE DATABASE IF NOT EXISTS " + hive_db)

    #Create a hive table with hiveContext from table registered in temptable
    hiveContext.sql("Drop table if exists " + hive_db + "." + hive_table)
    hiveContext.sql("Create Table "  + hive_db + "." + hive_table + " as select * from myfinaltemphv")
    print("\nCreation of HIVE table completed")
    
    #Read 2 rows from hive table and print the rows
    print("\nShow data from HIVE table")
    df_final_hv = hiveContext.sql("select * from "  + hive_db + "." + hive_table + " limit 2")
    df_final_hv.show()


# In[9]:


if __name__ == "__main__":
    conf = SparkConf().setAppName("Spark Products")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    hiveContext = HiveContext(sc)

    df_final = soln_Assignment3Q1()
    soln_Assignment3Q2(df_final)

