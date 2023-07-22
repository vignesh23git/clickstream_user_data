# Importing packages
import socket
import requests
import shutil
import os
from ip2geotools.databases.noncommercial import DbIpCity
from geopy.distance import distance
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql import Window

# Initializing spark session
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder \
                    .master('local[1]') \
                    .appName('clickstreamdata') \
                    .getOrCreate()
                    
partition_rn=Window.orderBy(lit("cst"))

# function to fetch city, country from ip address
def printDetails(ip):
    res = DbIpCity.get(ip, api_key="free")
    return res.city + "," + res.country    

printDetails_udf = udf(printDetails, StringType())

# read config file to fetch details on incremetal load
file_path="C:\\Users\\SATHYA\\Documents\\checkpoint.txt"
with open(file_path, 'r') as f:
    var_ck=int(f.read())

schema1='user_ID Integer, timestamp String,  URL String, IP_address String, user_agent_string String'

df=spark.read.csv("C:\\Users\\SATHYA\\Documents\\raw\\userkafkaoutnew.csv", schema=schema1)
var_count=df.count()
file_path="C:\\Users\\SATHYA\\Documents\\checkpoint.txt"
with open(file_path, 'w') as f:
    f.write(str(var_count)) 
  
# filter only new records  
df=df.withColumn("rn", row_number().over(partition_rn))
df=df.filter(df['rn'] > lit(var_ck)).drop("rn")


df1=df.withColumn("citycc", printDetails_udf(col("IP_address")))

df2=df1.withColumn("city", split("citycc", ',')[0]) \
.withColumn("country", split("citycc", ',')[1]) \
.drop("citycc", "IP_address")

df3=df2.withColumn("user_browser", split("user_agent_string", '_')[0]) \
.withColumn("OS", split("user_agent_string", '_')[1]) \
.withColumn("device", split("user_agent_string", '_')[2]) \
.drop("user_agent_string")

df4=df3.withColumn("rn", row_number().over(partition_rn))
df5=df4.withColumn("RowNumber", concat_ws("_", "rn", "timestamp")).drop("rn")

# aggregating country by URL
df6=df5.groupBy("URL","country").agg(count("user_ID").alias("number_of_clicks"), countDistinct("user_ID").alias("number_of_unique_users"))

#write as json file in processed container
df7=df6.toPandas()

def to_json_append(df,file):
    df.to_json('tmp.json',orient='records',lines=True)
    f=open('tmp.json','r')
    k=f.read()
    f.close()
    f=open(file,'a')
    f.write(k)
    f.close()
	
to_json_append(df7,'C:\\Users\\SATHYA\\Documents\\processed\\user_clickstream_data.json')












