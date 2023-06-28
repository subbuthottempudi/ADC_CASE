from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import when
from pyspark.sql.types import IntegerType,StringType,StructType,DoubleType,TimestampType
from pyspark.sql.functions import col
from dateutil.parser import parser, parse
import pyspark
import logging
import traceback
import sys
import os
import json
import re
import datetime

@udf
def convert_str_to_number(column):
    total_stars = 0
    num_map = {'K':1000, 'M':1000000, 'B':1000000000}
    if column.isdigit():
        total_stars = int(column)
    else:
        if len(column) > 1:
            total_stars = float(column[:-1]) * num_map.get(column[-1].upper(), 1)
    return int(total_stars)

@udf
def convert_str_to_months(x):
    str_months = x.split(".")
    months = (int(str_months[0])* 12)
    print(months)
    return months
    
@udf    
def convert_decimal_to_int(x):
    int_value = int(round(x))
    print(int_value)
    return int_value

@udf
def parse_date(dt_str):   
    default_date = datetime.datetime.strptime('01/01/01', '%m/%d/%y')
    dt = parse(dt_str, default = default_date)
    return dt


if __name__ == "__main__":

    # This is a pyspark docker container(jupyter), Spark context is already available if not please add it here
    #Logging configuration
    logging.basicConfig(filename='logs/debug.log',level=logging.DEBUG,format='%(asctime)s %(levelname)s:%(message)s', datefmt='%m/%d/%Y %H:%M:%S')
    logging.disable(logging.DEBUG)

    #sparksession  
    try:

        spark = SparkSession \
            .builder \
            .appName("ADC Data processing") \
            .config("spark.some.config.option", "ADC") \
            .getOrCreate()        
        
        logging.info('SparkSession created')
                    
    except:
        
        logging.error(str(traceback.format_exc()))
        sys.exit()

    logging.info('DataFrames created for each table/CSV file')
    api_homeownership_DF = spark.read.format("csv").option("header",True).load('input/api_homeownership.csv')
    api_oldcustomer_DF = spark.read.format("csv").option("header",True).load('input/api_oldcustomer.csv')
    api_newcustomer_DF = spark.read.format("csv").option("header",True).load('input/api_newcustomer.csv')
    api_purpose_DF = spark.read.format("csv").option("header",True).load('input/api_purpose.csv')
    api_subgrade_DF = spark.read.format("csv").option("header",True).load('input/api_subgrade.csv')
    api_verificationstatus_DF = spark.read.format("csv").option("header",True).load('input/api_verificationstatus.csv')
    api_state_DF = spark.read.format("csv").option("header",True).load('input/api_state.csv')

    logging.info('DataFrames Schemas for each table')
    api_homeownership_DF.printSchema()
    api_oldcustomer_DF.printSchema()
    api_newcustomer_DF.printSchema()
    api_purpose_DF.printSchema()
    api_subgrade_DF.printSchema()
    api_verificationstatus_DF.printSchema()
    api_state_DF.printSchema()
    

    logging.info('newcustomer_df after transformations')
    newcustomer_df = api_newcustomer_DF.withColumn("loan_amnt", convert_str_to_number(col('loan_amnt'))) \
        .withColumn("term", convert_str_to_months(col('term'))) \
        .withColumn("fico_range_low", convert_decimal_to_int(col('fico_range_low'))) \
        .withColumn("fico_range_high", convert_decimal_to_int(col('fico_range_high'))) \
        .withColumn("open_acc", convert_decimal_to_int(col('open_acc'))) \
        .withColumn("pub_rec", convert_decimal_to_int(col('pub_rec'))) \
        .withColumn("revol_bal", convert_decimal_to_int(col('revol_bal'))) \
        .withColumn("mort_acc", convert_decimal_to_int(col('mort_acc'))) \
        .withColumn("pub_rec_bankruptcies", convert_decimal_to_int(col('pub_rec_bankruptcies'))) \
        .withColumn("age", convert_decimal_to_int(col('age'))) \
        .withColumn("payment_status", convert_decimal_to_int(col('payment_status')))


    logging.info('Creating temp tables on spark dataframe to perform SQL')
    newcustomer_df.createOrReplaceTempView("newcustomer")
    api_homeownership_DF.createOrReplaceTempView("homeownership")
    api_purpose_DF.createOrReplaceTempView("purpose")
    api_subgrade_DF.createOrReplaceTempView("subgrade")
    api_verificationstatus_DF.createOrReplaceTempView("verificationstatus")
    api_state_DF.createOrReplaceTempView("state")
    api_oldcustomer_DF.createOrReplaceTempView("oldcustomer")
        

    logging.info('SparkSQl query to fetch the data from different table by joining ')   
    new_new_customer = spark.sql("""select new.id,IF(new.loan_status = 0, "Paid", "Written off") as loan_status_2,
                new.loan_status,new.loan_amnt,new.term,new.int_rate,new.installment,
                sub.name as sub_grade,new.employment_length as emp_length,home.name as home_ownership,
                new.annual_inc,vefify.name as verification_status, new.issued as issue_d,
                pur.name as purpose,st.name as addr_state, new.dti, new.fico_range_low, 
                new.fico_range_high, new.open_acc, new.pub_rec, new.revol_bal,
                new.revol_util,new.mort_acc,new.pub_rec_bankruptcies,new.age,
                new.payment_status as pay_status 
                from newcustomer new  
                left join subgrade sub on new.sub_grade_id = sub.id  
                left join homeownership home on new.home_ownership_id = home.id 
                left join verificationstatus vefify on new.verification_status_id = vefify.id 
                left join purpose pur on new.purpose_id = pur.id 
                left join state st on new.addr_state_id = st.id """)


    logging.info('Union query to merging the 2 data sets newcustomer and oldcustomer tables')  
    final_df = new_new_customer.union(api_oldcustomer_DF)


    logging.info('Adding the new derived columns to dataset')  
    final_df = final_df.withColumn("is_mortgage", when(final_df.home_ownership == "MORTGAGE","YES").otherwise("NO"))  \
                   .withColumn("is_rent", when(final_df.home_ownership == "RENT","YES").otherwise("NO")) \
                   .withColumn("is_own", when(final_df.home_ownership == "OWN","YES").otherwise("NO"))  \
                   .withColumn("is_any", when(final_df.home_ownership == "ANY","YES").otherwise("NO"))  \
                   .withColumn("is_other", when(final_df.home_ownership == "OTHER","YES").otherwise("NO")) \
                   .withColumn("is_verified", when(final_df.verification_status == "Verified","YES").otherwise("NO"))  \
                   .withColumn("is_not_verified", when(final_df.home_ownership == "Not Verified","YES").otherwise("NO"))  \
                   .withColumn("is_source_verified", when(final_df.home_ownership == "Source Verified","YES").otherwise("NO")) \
                   .drop("ID","loan_status_2")
    

    logging.info('Writing the dataset to CSV file')  
    final_df.coalesce(1).write.option("header",True) \
        .csv("output/final7")























