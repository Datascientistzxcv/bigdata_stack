from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
import re
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import concat
from pyspark.sql.functions import to_json,struct
import pandas as pd
def check(colType):
    [digits, decimals] = re.findall(r'\d+', colType)
    return 'float' if decimals == '0' else 'double'
def market_data():
    def projectArea(text):
        print(text)
        return str(text).replace(" ,",";").split(";")
    def priorityCommodities(text):
        return str(text).replace(", ",",").replace(";",",").split(",")
    jdbc_hostname="110.173.226.145"
    jdbc_port="49740"
    database="MakCorp"
    username="aws_dataload"
    password="MakCorp@2021#"
    spark = SparkSession.builder.master("local").appName("app name").config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.12.2").getOrCreate()
    jdbc_url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbc_hostname, jdbc_port, database)
    # data_table="dbo.API_MarketData2"
    data_table="API_Projects"
    connection_details = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }
    p_df = spark.read.jdbc(url=jdbc_url, table=data_table, properties=connection_details)
    p_df = p_df.select(
    [col(name) if 'decimal' not in colType else col(name).cast(check(colType)) for name, colType in p_df.dtypes]
    )
    # p_df.printSchema()
    p_df=p_df.withColumnRenamed("ASX Codes","ASXCODE").withColumnRenamed("Project Location State","Project_Location_State").withColumnRenamed("Project Stage","Project_Stage")
    m_df1=p_df.select("Project_Location_State","Project_Stage","ASXCODE")
    m_df2=m_df1.toPandas()
    pdfs=[]
    for i in m_df2.groupby('ASXCODE'):
        pdf=i[1][["Project_Location_State","Project_Stage","ASXCODE"]].iloc[0:1]
        pdfs.append(pdf.to_dict('records')[0])
    df = pd.DataFrame(pdfs)
    sparkDF=spark.createDataFrame(df) 
    sparkDF=sparkDF.selectExpr("Project_Location_State","Project_Stage","ASXCODE")
    jdbc_hostname="110.173.226.145"
    jdbc_port="49740"
    database="MakCorp"
    username="aws_dataload"
    password="MakCorp@2021#"
    spark = SparkSession.builder.master("local").appName("app name").config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.12.2").getOrCreate()
    jdbc_url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbc_hostname, jdbc_port, database)
    # data_table="dbo.API_MarketData2"
    data_table="API_MarketData2"
    connection_details = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }
    m_df = spark.read.jdbc(url=jdbc_url, table=data_table, properties=connection_details)
    m_df = m_df.select(
    [col(name) if 'decimal' not in colType else col(name).cast(check(colType)) for name, colType in m_df.dtypes]
    )
    spark_fil=m_df.join(sparkDF,m_df.ASX  ==  sparkDF.ASXCODE,"left")
    spark_fil=spark_fil.sort(spark_fil.ASX)
    spark_fil=spark_fil.drop("ASXCODE")
    spark_fil=spark_fil.withColumnRenamed("Project_Location_State","Project Location State").withColumnRenamed("Project_Stage","Project Stage")
    projectArea_udf = udf(projectArea,ArrayType(StringType()))
    spark_fil.na.fill(value=0).show()
    priorityCommodities_udf=udf(priorityCommodities,ArrayType(StringType()))
    spark_fil.withColumn("ProjectArea",projectArea_udf(spark_fil.ProjectArea)).withColumn("ProjectCountry",projectArea_udf(spark_fil.ProjectCountry)).withColumn("Priority Commodities",priorityCommodities_udf(col("Priority Commodities"))).write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('marketdata_public')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    spark_fil.withColumn("ProjectArea",projectArea_udf(spark_fil.ProjectArea)).withColumn("ProjectCountry",projectArea_udf(spark_fil.ProjectCountry)).withColumn("Priority Commodities",priorityCommodities_udf(col("Priority Commodities"))).write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('marketdata_public_tier1')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    spark_fil.withColumn("ProjectArea",projectArea_udf(spark_fil.ProjectArea)).withColumn("ProjectCountry",projectArea_udf(spark_fil.ProjectCountry)).withColumn("Priority Commodities",priorityCommodities_udf(col("Priority Commodities"))).write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('marketdata_public_tier2')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    spark_fil.withColumn("ProjectArea",projectArea_udf(spark_fil.ProjectArea)).withColumn("ProjectCountry",projectArea_udf(spark_fil.ProjectCountry)).withColumn("Priority Commodities",priorityCommodities_udf(col("Priority Commodities"))).write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_marketdata_tier1')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    spark_fil.withColumn("ProjectArea",projectArea_udf(spark_fil.ProjectArea)).withColumn("ProjectCountry",projectArea_udf(spark_fil.ProjectCountry)).withColumn("Priority Commodities",priorityCommodities_udf(col("Priority Commodities"))).write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_marketdata_tier2')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
with DAG('MakCorp_market_data_dag', description='MarketData DAG', start_date=datetime(2018, 11, 1), catchup=False) as dag:
    start= DummyOperator(task_id='Market_Data_Loading_Started')
    market_data_task	= PythonOperator(task_id='ASX_MarketData', python_callable=market_data)
    end= DummyOperator(task_id='Market_Data_Loading_Completed')
    start >> market_data_task >>end 