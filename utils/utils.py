import json, os, re, sys, time
from typing import Any, Callable, Optional
import pyspark
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

class SparkClass :

 
    def __init__(self):
        pass

   
    def sparkStart(self, kwargs:dict) -> SparkSession:
        """ return spark session with configs from config.json  """
        #print(kwargs)
        master = kwargs['spark_conf']['master']
        appname = kwargs['spark_conf']['appname']
        spark = SparkSession.builder.master(f"{master}").appName(f"{appname}").getOrCreate()
        return spark

    def importDataToDF(self, spark:SparkSession, datapath:str) -> DataFrame:
        "Import data from datapath into dataframe and return the dataframe"
        
        df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(datapath)
        return df
    
    def exportDataToFile(self, spark:SparkSession, df:DataFrame, datapath:str, filename:str) -> None :
        "export dataframe df to destination datapath and filename"
        
        sc = spark._sc
        
        df.repartition(1).write.mode("overwrite").format("csv").option("header","true").save(datapath+"temp/")
        
        hadoopConfiguration = sc._jsc.hadoopConfiguration()
        hadoopPackage = sc._gateway.jvm.org.apache.hadoop
        sc_path = hadoopPackage.fs.Path
        FsPermission = hadoopPackage.fs.permission.FsPermission
        IOUtils = hadoopPackage.io.IOUtils
        
        p = sc_path(datapath+"temp/")
        fs = p.getFileSystem(hadoopConfiguration)
        status = fs.listStatus(sc_path(datapath+"temp/"))
        for fileStatus in status:
            temp = fileStatus.getPath().toString()
        if "part" in temp:
                src_path = sc_path(temp)
            
        p1 = sc_path(str(src_path))
        p2 = sc_path(datapath + filename)
        
        fs.rename(p1, p2)
        
        
        
    
