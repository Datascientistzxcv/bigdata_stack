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
def check(colType):
    [digits, decimals] = re.findall(r'\d+', colType)
    return 'float' if decimals == '0' else 'double'
def market_data():
    jdbc_hostname="110.173.226.145"
    jdbc_port="49740"
    database="MakCorp"
    username="aws_dataload"
    password="MakCorp@2021#"
    spark = SparkSession.builder.master("local").appName("app name").config(conf=SparkConf()).config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.12.2").getOrCreate()
    jdbc_url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbc_hostname, jdbc_port, database)
    data_table="dbo.API_MarketData2"
    connection_details = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }
    def projectArea(text):
        print(text)
        return str(text).replace(" ,",";").split(";")
    def priorityCommodities(text):
        return str(text).replace(", ",",").replace(";",",").split(",")

    df = spark.read.jdbc(url=jdbc_url, table=data_table, properties=connection_details)
    df = df.select(
    [col(name) if 'decimal' not in colType else col(name).cast(check(colType)) for name, colType in df.dtypes]
)
    projectArea_udf = udf(projectArea,ArrayType(StringType()))
    priorityCommodities_udf=udf(priorityCommodities,ArrayType(StringType()))
    df.withColumn("ProjectArea",projectArea_udf(df.ProjectArea)).withColumn("ProjectCountry",projectArea_udf(df.ProjectCountry)).withColumn("Priority Commodities",priorityCommodities_udf(col("Priority Commodities"))).write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('marketdata_public')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    df.withColumn("ProjectArea",projectArea_udf(df.ProjectArea)).withColumn("ProjectCountry",projectArea_udf(df.ProjectCountry)).withColumn("Priority Commodities",priorityCommodities_udf(col("Priority Commodities"))).write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('marketdata_public_tier1')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    df.withColumn("ProjectArea",projectArea_udf(df.ProjectArea)).withColumn("ProjectCountry",projectArea_udf(df.ProjectCountry)).withColumn("Priority Commodities",priorityCommodities_udf(col("Priority Commodities"))).write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('marketdata_public_tier2')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    df.withColumn("ProjectArea",projectArea_udf(df.ProjectArea)).withColumn("ProjectCountry",projectArea_udf(df.ProjectCountry)).withColumn("Priority Commodities",priorityCommodities_udf(col("Priority Commodities"))).write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_marketdata_tier1')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    df.withColumn("ProjectArea",projectArea_udf(df.ProjectArea)).withColumn("ProjectCountry",projectArea_udf(df.ProjectCountry)).withColumn("Priority Commodities",priorityCommodities_udf(col("Priority Commodities"))).write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_marketdata_tier2')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
with DAG('MakCorp_market_data_dag', description='MarketData DAG', start_date=datetime(2018, 11, 1), catchup=False) as dag:
    start= DummyOperator(task_id='Market_Data_Loading_Started')
    market_data_task	= PythonOperator(task_id='ASX_MarketData', python_callable=market_data)
    end= DummyOperator(task_id='Market_Data_Loading_Completed')
    start >> market_data_task >>end 