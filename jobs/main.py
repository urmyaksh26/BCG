
import json, os, re, sys
from typing import Callable, Optional

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, explode
from pyspark.sql.window import Window
from pyspark.sql import functions as f

sys.path.insert(1, project_dir)
from utils import utils

def main():
    """ purpose of this application is to,
        1. import vehile accidents raw data into a spark dataframe
        2. Apply transformation to get results
        3. export the dataframe result 
    """

    # import json config file
    conf = openConfig(f"{project_dir}/config.json")
    
    # Create SparkClass Object from utils
    sparkClass = utils.SparkClass()
    
    #Create SparkSession object
    spark = sparkClass.sparkStart(conf)
    
    # Import all the input data into dataframe, Input path is defined in the config.json file
    df_charges = sparkClass.importDataToDF(spark, conf["input_file"]["Charges"])
    df_damages = sparkClass.importDataToDF(spark, conf["input_file"]["Damages"])
    df_primary_person = sparkClass.importDataToDF(spark, conf["input_file"]["Primary_Person"])
    df_units = sparkClass.importDataToDF(spark, conf["input_file"]["Units"])
    
    # Start Analytics
    
    # Analysis 1: Find the number of crashes (accidents) in which number of persons killed are male?
    numMaleKilled(sparkClass,spark,df_primary_person,conf["output_file"]["output1"]["path"],conf["output_file"]["output1"]["filename"])
    
    # Analysis 2: How many two-wheelers are booked for crashes?
    numTwoWheelersCrashes(sparkClass,spark,df_units,conf["output_file"]["output2"]["path"],conf["output_file"]["output2"]["filename"])
    
    #Analysis 3: Which state has the highest number of accidents in which females are involved? 
    stateHighestNumAccidentsFemale(sparkClass,spark,df_primary_person,conf["output_file"]["output3"]["path"],conf["output_file"]["output3"]["filename"])
    
    #Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    top5To15Vehicle(sparkClass,spark,df_units,conf["output_file"]["output4"]["path"],conf["output_file"]["output4"]["filename"])
    
    #Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style 
    bodyStylesWithTopEthnicUserGroup(sparkClass,spark,df_units,df_primary_person,conf["output_file"]["output5"]["path"],conf["output_file"]["output5"]["filename"])
    
    #Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
    topZipCodesNumCrashesWithAlcohal(sparkClass,spark,df_units,df_primary_person,conf["output_file"]["output6"]["path"],conf["output_file"]["output6"]["filename"])
    
    #Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance  
    countCrashWithNoDamageAvailsInsurance(sparkClass,spark,df_units,df_damages,conf["output_file"]["output7"]["path"],conf["output_file"]["output7"]["filename"])
    
    #Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
    top5MakesDriversCharged(sparkClass,spark,df_units,df_charges,df_primary_person,conf["output_file"]["output8"]["path"],conf["output_file"]["output8"]["filename"])

    
def openConfig(filepath:str) -> dict :
    "Open config.json and return config in dictionary"
    if os.path.exists(filepath): 
        with open(filepath, "r") as f:
                data = json.load(f)
        return data
    else :
        print(f"{filepath} does not exist ") 
        exit()
    
def numMaleKilled(sparkClass:object,spark:SparkSession,df_primary_person:DataFrame,output_path:str,output_filename:str) -> None:
    "Export number of crashes (accidents) in which number of persons killed are male?"
    
    cnt = df_primary_person.filter((df_primary_person['PRSN_GNDR_ID'] == "MALE") & (df_primary_person['PRSN_INJRY_SEV_ID']=="KILLED")).count()
    data = [{'num_male_killed' : cnt}]
    df = spark.createDataFrame(data)
    sparkClass.exportDataToFile(spark,df,output_path,output_filename)
    
    
def numTwoWheelersCrashes(sparkClass:object,spark:SparkSession,df_units:DataFrame,output_path:str,output_filename:str) -> None:
    "Export number of two-wheelers are booked for crashes"
    
    cnt = df_units.filter((df_units['VEH_BODY_STYL_ID'] == "MOTORCYCLE")).count()
    data = [{'num_Two_Wheelers_Crashes' : cnt}]
    df = spark.createDataFrame(data)
    sparkClass.exportDataToFile(spark,df,output_path,output_filename)
    
def stateHighestNumAccidentsFemale(sparkClass:object,spark:SparkSession,df_primary_person:DataFrame,output_path:str,output_filename:str) -> None:
    "Export state which has the highest number of accidents in which females are involved"
    
    df = df_primary_person.filter((df_primary_person['PRSN_GNDR_ID'] == "FEMALE")).groupby('DRVR_LIC_STATE_ID').\
                           agg(f.count('PRSN_GNDR_ID').alias("count"))
    df = df.withColumn("rnk",f.rank().over(Window.orderBy(f.col("count").desc()))).filter("rnk=1").select('DRVR_LIC_STATE_ID','count')
    sparkClass.exportDataToFile(spark,df,output_path,output_filename)
    
                    
def top5To15Vehicle(sparkClass:object,spark:SparkSession,df_units:DataFrame,output_path:str,output_filename:str) -> None:
    "Export top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death"
    
    df =df_units.filter("VEH_MAKE_ID != 'NA'").withColumn("sum_injury_cnt",f.col('TOT_INJRY_CNT') + f.col('DEATH_CNT')).\
                                    groupby('VEH_MAKE_ID').agg(f.sum((f.col('sum_injury_cnt'))).alias("total_injury_cnt")).\
                                    withColumn("rnk",f.dense_rank().over(Window.orderBy(f.col('total_injury_cnt').desc()))).\
                                    filter("rnk between 5 and 15").drop('rnk')
    
    sparkClass.exportDataToFile(spark,df,output_path,output_filename)
    
def bodyStylesWithTopEthnicUserGroup(sparkClass:object,spark:SparkSession,df_units:DataFrame,df_primary_person:DataFrame,output_path:str,output_filename:str) -> None:
    "Export top ethnic user group of each unique body style which involved in crashes"
    
    df = df_units.join(df_primary_person, on=['CRASH_ID'], how='inner'). \
            filter(~df_units['VEH_BODY_STYL_ID'].isin(["NA", "UNKNOWN", "NOT REPORTED",
                                                         "OTHER  (EXPLAIN IN NARRATIVE)"])). \
            filter(~df_primary_person['PRSN_ETHNICITY_ID'].isin(["NA", "UNKNOWN"])). \
            groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count(). \
            withColumn("row", f.row_number().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(f.col("count").desc()))).\
            filter(f.col("row") == 1).drop('row')
    sparkClass.exportDataToFile(spark,df,output_path,output_filename)
    
def topZipCodesNumCrashesWithAlcohal(sparkClass:object,spark:SparkSession,df_units:DataFrame,df_primary_person:DataFrame,output_path:str,output_filename:str) -> None:
    "Export Top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash"
    
    df = df_units.join(df_primary_person, on=['CRASH_ID'], how='inner'). \
            dropna(subset=["DRVR_ZIP"]). \
            filter((f.col("CONTRIB_FACTR_1_ID").contains("ALCOHOL")) | (f.col("CONTRIB_FACTR_2_ID").contains("ALCOHOL"))). \
            groupby("DRVR_ZIP").count().orderBy(f.col("count").desc()).limit(5)
    
    sparkClass.exportDataToFile(spark,df,output_path,output_filename)
    

def countCrashWithNoDamageAvailsInsurance(sparkClass:object,spark:SparkSession,df_units:DataFrame,df_damages:DataFrame,output_path:str,output_filename:str) -> None:
    "Export count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance"
    
    cnt = df_damages.join(df_units, on=["CRASH_ID"], how='inner'). \
        filter(((df_units['VEH_DMAG_SCL_1_ID'].contains("DAMAGED")) & (df_units['VEH_DMAG_SCL_1_ID'] > "DAMAGED 4")) | \
               (((df_units['VEH_DMAG_SCL_2_ID'].contains("DAMAGED")) & (df_units['VEH_DMAG_SCL_2_ID'] > "DAMAGED 4")))). \
        filter(df_damages['DAMAGED_PROPERTY'] == "NONE"). \
        filter(df_units['FIN_RESP_TYPE_ID'] == "PROOF OF LIABILITY INSURANCE").count()
    data = [{'count_Crash_With_NoDamage_Avails_Insurance' : cnt}]
    df = spark.createDataFrame(data)
    sparkClass.exportDataToFile(spark,df,output_path,output_filename)
    
def top5MakesDriversCharged(sparkClass:object,spark:SparkSession,df_units:DataFrame,df_charges:DataFrame,df_primary_person:DataFrame,output_path:str,output_filename:str) -> None:
    
    """"Export Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences"""
    
    top_25_state_list = [row[0] for row in df_units.filter(f.col("VEH_LIC_STATE_ID").cast("int").isNull()).
            groupby("VEH_LIC_STATE_ID").count().orderBy(f.col("count").desc()).limit(25).collect()]
    top_10_used_vehicle_colors = [row[0] for row in df_units.filter(df_units.VEH_COLOR_ID != "NA").
            groupby("VEH_COLOR_ID").count().orderBy(f.col("count").desc()).limit(10).collect()]
    
    df = df_charges.join(df_primary_person, on=['CRASH_ID'], how='inner'). \
            join(df_units, on=['CRASH_ID'], how='inner'). \
            filter(df_charges['CHARGE'].contains("SPEED")). \
            filter(df_primary_person['DRVR_LIC_TYPE_ID'].isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])). \
            filter(df_units['VEH_COLOR_ID'].isin(top_10_used_vehicle_colors)). \
            filter(df_units['VEH_LIC_STATE_ID'].isin(top_25_state_list)). \
            groupby("VEH_MAKE_ID").count(). \
            orderBy(f.col("count").desc()).limit(5)
    
    sparkClass.exportDataToFile(spark,df,output_path,output_filename)

    
if __name__ == '__main__':
    main()
