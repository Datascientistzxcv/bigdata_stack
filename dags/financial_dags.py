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
from pyspark.sql.functions import date_format
from pyspark.sql.types import*
from pyspark.sql.functions import to_timestamp
from pyspark.sql import functions as F
from pyspark.sql.functions import *

def check(colType):
    [digits, decimals] = re.findall(r'\d+', colType)
    return 'float' if decimals == '0' else 'double'
def financial_data():
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
    # fin_df=fin_df.where(F.col("Period").like('%2022Q2%') | F.col("Period").like('%2022Q1%') | F.col("Period").like('%2022Q3%') | F.col("Period").like('%2022Q4%'))
    fin_df=fin_df.where(F.col("Period").like('%Q2%') | F.col("Period").like('%Q1%') | F.col("Period").like('%Q3%') | F.col("Period").like('%Q4%'))

    jdbc_hostname="110.173.226.145"
    jdbc_port="49740"
    database="MakCorp"
    username="aws_dataload"
    password="MakCorp@2021#"
    spark = SparkSession.builder.master("local").appName("app name").config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.12.2").getOrCreate()
    jdbc_url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbc_hostname, jdbc_port, database)
    data_table="dbo.API_MarketData2"
    # data_table="API_MarketData2"
    connection_details = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }
    m_df = spark.read.jdbc(url=jdbc_url, table=data_table, properties=connection_details)
    m_df = m_df.select(
    [col(name) if 'decimal' not in colType else col(name).cast(check(colType)) for name, colType in m_df.dtypes]
    )
    # m_df.drop("MarketDataID").dropDuplicates().distinct().count()
    # m_df1=m_df.select('CUST_ASX_041426558','CUST_StartMarketCap_031042920').dropna()
    m_df1=m_df.selectExpr('ASX as CUST_ASX_041426558','MarketCap as CUST_StartMarketCap_031042920').dropna()
    m_df2=m_df1.toPandas()
    pdfs=[]
    for i in m_df2.groupby('CUST_ASX_041426558'):
        pdf=i[1][['CUST_StartMarketCap_031042920','CUST_ASX_041426558']].iloc[0:1]
        pdfs.append(pdf.to_dict('records')[0])
    df = pd.DataFrame(pdfs)
    sparkDF=spark.createDataFrame(df) 
    sparkDF=sparkDF.selectExpr("CUST_StartMarketCap_031042920 as MarketCap","CUST_ASX_041426558 as CUST_ASX_041426558")
    spark_fil=fin_df.join(sparkDF,fin_df.ASX  ==  sparkDF.CUST_ASX_041426558,"right")
    spark_fil=spark_fil.sort(spark_fil.ASX)
    spark_fil=spark_fil.drop("ASX")
    spark_fil=spark_fil.withColumnRenamed("CUST_ASX_041426558","ASX")
    spark_fil=spark_fil.drop("FinancialsID")
    # spark_fil=spark_fil.dropDuplicates() #43714
    spark_fil=spark_fil.withColumn('con', F.explode(F.array('ProjectArea'))).withColumn('con1', F.explode(F.array('ProjectCountry')))   \
                .groupby(
                'EditDate',
                'Period',
                'ASX',
                'AnnDate',
                'NetOperatingCashFlow',
                'CustomerIncome',
                'OtherIncome',
                'ExplSpend',
                'DevProdSpend',
                'StaffCosts',
                'AdminCosts',
                'OtherCosts',
                'NetCashInvest',
                'CashflowNegative',
                'CashflowPositive',
                'CashflowTotal',
                'CashflowFin31',
                'CashflowFin35_39',
                'BankBalance',
                'Debt',
                'ForecastNetOperating',
                'ForecastIncome',
                'ForecastExplSpend',
                'ForecastDevProdSpend',
                'ForecastStaffCost',
                'ForecastAdminCost',
                'AnnWebLink',
                'Best Project Stage',
                'Contact',
                'Title',
                'Priority Commodities',
                'CRDate',
                'CRAmount',
                'Salaries',
                'EmployeeBenefits',
                'ConsultingFees',
                'Exploration',
                'Production',
                'AllOtherCosts',
                'Income',
                'ProfitandLoss',
                'EarnperShare',
                'Assets',
                'Liabilities',
                'Equity',
                'MarketCap').agg(F.collect_set('con').alias('Project Location Area'),F.collect_set('con1').alias('ProjectCountry'))
    spark_fil=spark_fil.filter(F.col("Project Location Area")!=F.array(F.lit('')))
    def priorityCommodities(text):
        return str(text).replace(", ",",").replace(";",",").split(",")
    priorityCommodities_udf=udf(priorityCommodities,ArrayType(StringType()))
    # spark_fil = spark_fil.withColumn('Project Spend', abs(expr("ExplSpend + DevProdSpend")))
    spark_fil = spark_fil.withColumn('ProjectSpending', abs(expr("ExplSpend + DevProdSpend"))).withColumn('NetOperatingCashFlow', abs(col("NetOperatingCashFlow"))).withColumn('CustomerIncome', abs(col("CustomerIncome"))).withColumn('OtherIncome', abs(col("OtherIncome"))).withColumn('ExplSpend', abs(col("ExplSpend"))).withColumn('DevProdSpend', abs(col("DevProdSpend"))).withColumn('StaffCosts', abs(col("StaffCosts"))).withColumn('AdminCosts', abs(col("AdminCosts"))).withColumn('OtherCosts', abs(col("OtherCosts"))).withColumn('ForecastNetOperating', abs(col("ForecastNetOperating"))).withColumn('ForecastIncome', abs(col("ForecastIncome"))).withColumn('ForecastExplSpend', abs(col("ForecastExplSpend"))).withColumn('ForecastDevProdSpend', abs(col("ForecastDevProdSpend"))).withColumn('ForecastStaffCost', abs(col("ForecastStaffCost"))).withColumn('ForecastAdminCost', abs(col("ForecastAdminCost")))
    spark_fil.withColumn("Priority Commodities",priorityCommodities_udf(col("Priority Commodities"))).write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_financials_tier1')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.mapping.rich.date", "false").option("es.nodes.wan.only","true").mode("overwrite").save()
    spark_fil.withColumn("Priority Commodities",priorityCommodities_udf(col("Priority Commodities"))).write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_financials_tier2')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.mapping.rich.date", "false").option("es.nodes.wan.only","true").mode("overwrite").save()
    # spark_fil.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_financials_test_tier1')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    # spark_fil.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_financials_test_tier2')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
with DAG('MakCorp_financial_data_dag', description='financial DAG', start_date=datetime(2018, 11, 1), catchup=False) as dag:
    start= DummyOperator(task_id='financial_Data_Loading_Started')
    financial_data_task	= PythonOperator(task_id='ASX_financial', python_callable=financial_data)
    end= DummyOperator(task_id='financial_Data_Loading_Completed')
    start >> financial_data_task >>end 

