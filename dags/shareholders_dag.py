from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
import re
import pandas as pd
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import concat
from pyspark.sql.functions import to_json,struct
from pyspark.sql import functions as F
from pyspark.sql.functions import *
def check(colType):
    [digits, decimals] = re.findall(r'\d+', colType)
    return 'float' if decimals == '0' else 'double'
def shareholder_data():
    jdbc_hostname="110.173.226.145"
    jdbc_port="49740"
    database="MakCorp"
    username="aws_dataload"
    password="MakCorp@2021#"
    spark = SparkSession.builder.master("local").appName("app name").config(conf=SparkConf()).config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.12.2").getOrCreate()
    jdbc_url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbc_hostname, jdbc_port, database)
    data_table="dbo.API_Shareholders"
    connection_details = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }

    s_df = spark.read.jdbc(url=jdbc_url, table=data_table, properties=connection_details)
    s_df = s_df.withColumnRenamed("ASXCode","ASX")
    s_df = s_df.select(
    [col(name) if 'decimal' not in colType else col(name).cast(check(colType)) for name, colType in s_df.dtypes]
)
    # df=df.dropDuplicates(subset = ['Contact'])
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
    p_df=p_df.withColumnRenamed("ASX Codes","ASXCODE").withColumnRenamed("Project Stage","Project_Stage").withColumnRenamed("Project Name","Project_Name")
    m_df1=p_df.select("Project_Stage","ASXCODE","Project_Name")
    m_df2=m_df1.toPandas()
    pdfs=[]
    for i in m_df2.groupby('ASXCODE'):
    #     print(i[1])
        pdf=i[1][["Project_Stage","ASXCODE","Project_Name"]].iloc[0:1]
        pdfs.append(pdf.to_dict('records')[0])
    df = pd.DataFrame(pdfs)
    sparkDF=spark.createDataFrame(df) 
    sparkDF=sparkDF.selectExpr("Project_Stage","ASXCODE","Project_Name")
    spark_fil2=sparkDF.join(s_df,sparkDF.ASXCODE  ==  s_df.ASX,"left")
    spark_fil2=spark_fil2.sort(spark_fil2.ASX)
    spark_fil2=spark_fil2.drop("ASX")
    spark_fil2=spark_fil2.withColumnRenamed("ASXCODE","ASX").withColumnRenamed("Project_Stage","ProjectStage").withColumnRenamed("Project_Name","Project Name")
    
    jdbc_hostname="110.173.226.145"
    jdbc_port="49740"
    database="MakCorp"
    username="aws_dataload"
    password="MakCorp@2021#"
    spark = SparkSession.builder.master("local").appName("app name").config(conf=SparkConf()).config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.12.2").getOrCreate()
    jdbc_url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbc_hostname, jdbc_port, database)
    data_table="dbo.API_Financials"
    connection_details = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }

    fin_df = spark.read.jdbc(url=jdbc_url, table=data_table, properties=connection_details)
    fin_df = fin_df.select(
    [col(name) if 'decimal' not in colType else col(name).cast(check(colType)) for name, colType in fin_df.dtypes]
    )
    fin_df=fin_df.where(F.col("Period").like('%Q2%') | F.col("Period").like('%Q1%') | F.col("Period").like('%Q3%') | F.col("Period").like('%Q4%'))
    fin_df=fin_df.drop("FinancialsID")
    m_df1=fin_df.selectExpr("ASX as ASXCODE","BankBalance","ExplSpend","DevProdSpend")
    m_df2=m_df1.toPandas()
    pdfs=[]
    for i in m_df2.groupby('ASXCODE'):
        pdf=i[1][["BankBalance","ASXCODE","ExplSpend","DevProdSpend"]].iloc[0:1]
        pdfs.append(pdf.to_dict('records')[0])
    df = pd.DataFrame(pdfs)
    sparkDF=spark.createDataFrame(df) 
    sparkDF=sparkDF.selectExpr("BankBalance","ASXCODE","ExplSpend","DevProdSpend")
    spark_fil=sparkDF.join(spark_fil2,sparkDF.ASXCODE  ==  spark_fil2.ASX,"right")
    spark_fil=spark_fil.drop("ASXCODE")
    spark_fil=spark_fil.withColumn('con', F.explode(F.array('Project Location Area'))).withColumn('con1', F.explode(F.array('Project Location Country')))   \
                .groupby(
                 'BankBalance',
                 'ExplSpend',
                 'DevProdSpend',
                 'ERP',
                 'ASX',
                 'ProjectsID',
                 'CommoditiesID',
                 'EditDate',
                 'CompanyID',
                 'TSX Codes',
                 'Project Name',
                 'Project Stage',
                 'Est Mine Life',
                 'Joint Venture Info',
                 'Cost per tonne',
                 'Sale Price per tonne',
                 'Net Project Value',
                 'Open Pit',
                 'Underground',
                 'Project Web Page',
                 'Resource Units',
                 'Resource Indicated',
                 'Resource Inferred',
                 'Resource Measured',
                 'Resource Actual Amount',
                 'Mining Licence Approved',
                 'Environmental Approval',
                 'Project Announcement',
                 'Project Start Date',
                 'Quarterly Announcement',
                 'Updated By',
                 'Notes',
                 'Commodity Type',
                 'Commodity Mine Type',
                 'Commodity Units',
                 'Commodity Indicated',
                 'Commodity Inferred',
                 'Commodity Measured',
                 'Commodity Proven Total',
                 'Commodity Probable Total',
                 'Commodity Proven Contained',
                 'Commodity Probable Contained',
                 'Commodity Proven Grade',
                 'Commodity Probable Grade',
                 'Commodity Total Resource',
                 'Commodity Grade Units',
                 'Commodity Grade Indicated',
                 'Commodity Grade Inferred',
                 'Commodity Grade Measured',
                 'Commodity Cut Off',
                 'Commodity Production Cost per Unit',
                 'Commodity Production Qty',
                 'Commodity Production Unit',
                 'Commodity Production Frequency',
                 'Commodity Production YTD',
                 'Commodity Production Currency',
                 'Commodity Production Grade',
                 'Commodity Strip Ratio',
                 'Processed Tonnes',
                 'Processed Grade',
                 'Processed Commodity Produced',
                 'Processed Commodity Sold',
                 'Proven Probable Total',
                 'Total Ore Mined',
                 'Total Mined Grade',
                 'Cost Stockpile per unit',
                 'Cost Mining per unit',
                 'Cost Total per unit',
                 'Commodity Notes',
                 'Drilling Periods',
                 'Project Location Belt',
                 'Project Location City',
                 'Project Location State',
                 'Project Location Continent',
                 'Drill Grade Low',
                 'Drill Grade High',
                 'Drill Width Low',
                 'Drill Width High',
                 'Drill Depth Low',
                 'Drill Depth High',
                 'Drill Length Low',
                 'Drill Length High',
                 'Drill Ann Date',
                 'Drill Ann Link',
                 'Project Updated',
                 'Deposit Type',
                 'Project Industry',
                 'Project Area Size',
                 'Drill Metres Drilled',
                 'Drill Intercepts Low',
                 'Drill Intercepts High',
                 'Drill Type',
                 'Priority Commodities',
                 'Project Percent Ownership',
                 'QT Expl Spend',
                 'QT Income',
                 'QT Prod Spend',
                 'QT Staff Costs',
                 'QT Staff and Admin Costs',
                 'MarketCap',
                 'ProjectSpending').agg(F.collect_set('con').alias('Project Location Area'),F.collect_set('con1').alias('Project Location Country'))
    spark_fil = spark_fil.filter(F.col("Project Location Area")!=F.array(F.lit('')))
    spark_fil = spark_fil.withColumn('ProjectSpending', expr("ExplSpend + DevProdSpend"))
    spark_fil.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_shareholders_tier1')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    spark_fil.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_shareholders_tier2')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    
    # spark_fil.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_test_shareholders_tier1')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    # spark_fil.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_test_shareholders_tier2')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
with DAG('MakCorp_Shareholder_data_dag', description='Shareholder DAG', start_date=datetime(2018, 11, 1), catchup=False) as dag:
    start= DummyOperator(task_id='Shareholder_Data_Loading_Started')
    shareholder_task	= PythonOperator(task_id='ASX_Shareholder', python_callable=shareholder_data)
    end= DummyOperator(task_id='Shareholder_Data_Loading_Completed')
    start >> shareholder_task >>end 