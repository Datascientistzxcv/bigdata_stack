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
def capitalraises_data():
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
    data_table="API_CapitalRaise"
    connection_details = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }
    m_df = spark.read.jdbc(url=jdbc_url, table=data_table, properties=connection_details)
    m_df = m_df.select(
    [col(name) if 'decimal' not in colType else col(name).cast(check(colType)) for name, colType in m_df.dtypes]
    )
    # m_df=m_df.withColumnRenamed("ASX Code","ASX")
    spark_fil2=sparkDF.join(m_df,sparkDF.ASXCODE  ==  m_df.ASX,"left")
    spark_fil2=spark_fil2.sort(spark_fil2.ASX)
    spark_fil2=spark_fil2.drop("ASX")
    spark_fil2=spark_fil2.withColumnRenamed("Project_Location_State","Project Location State").withColumnRenamed("Project_Stage","Project Stage")
    # spark_fil2.na.fill(value=0).distinct().count()
    spark_fil2=spark_fil2.withColumnRenamed("ASXCODE","ASX")
    #fin
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
    fin_df=fin_df.withColumn('con', F.explode(F.array('ProjectArea'))).withColumn('con1', F.explode(F.array('ProjectCountry')))   \
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
                    'Equity').agg(F.collect_set('con').alias('Project Location Area'),F.collect_set('con1').alias('ProjectCountry'))
    fin_df = fin_df.filter(F.col("Project Location Area")!=F.array(F.lit('')))
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
    spark_fil=spark_fil.sort(spark_fil.ASX)
    spark_fil=spark_fil.drop("ASXCODE")
    spark_fil = spark_fil.withColumn('ProjectSpending', expr("ExplSpend + DevProdSpend"))
    spark_fil.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('capitalraises')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    spark_fil.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('capitalraises_tier1')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    spark_fil.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('capitalraises_tier2')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    spark_fil.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_capitalraises')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    spark_fil.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_capitalraises_tier1')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
    spark_fil.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_capitalraises_tier2')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
with DAG('MakCorp_CapitalRaise_data_dag', description='CapitalRaise DAG', start_date=datetime(2018, 11, 1), catchup=False) as dag:
    start= DummyOperator(task_id='CapitalRaise_Data_Loading_Started')
    capitalraises_task	= PythonOperator(task_id='ASX_Capitalraises', python_callable=capitalraises_data)
    end= DummyOperator(task_id='CapitalRaise_Data_Loading_Completed')
    start >> capitalraises_task >>end 




# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
# from pyspark import SparkConf
# from pyspark.sql import SQLContext
# from pyspark import SparkContext
# from pyspark.sql import SparkSession
# import re
# import pandas as pd
# from pyspark.sql.types import ArrayType, StringType
# from pyspark.sql.functions import udf
# from pyspark.sql.functions import lit
# from pyspark.sql.functions import col
# from pyspark.sql.functions import concat
# from pyspark.sql.functions import to_json,struct
# def check(colType):
#     [digits, decimals] = re.findall(r'\d+', colType)
#     return 'float' if decimals == '0' else 'double'
# def capitalraises_data():
#     jdbc_hostname="110.173.226.145"
#     jdbc_port="49740"
#     database="MakCorp"
#     username="aws_dataload"
#     password="MakCorp@2021#"
#     spark = SparkSession.builder.master("local").appName("app name").config(conf=SparkConf()).config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.12.2").getOrCreate()
#     jdbc_url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbc_hostname, jdbc_port, database)
#     data_table="dbo.API_CapitalRaise"
#     connection_details = {
#         "user": username,
#         "password": password,
#         "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
#     }

#     df = spark.read.jdbc(url=jdbc_url, table=data_table, properties=connection_details)
#     df = df.select(
#     [col(name) if 'decimal' not in colType else col(name).cast(check(colType)) for name, colType in df.dtypes]
# )
#     df.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('capitalraises')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
#     df.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('capitalraises_tier1')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
#     df.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('capitalraises_tier2')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
#     df.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_capitalraises')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
#     df.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_capitalraises_tier1')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
#     df.write.format("org.elasticsearch.spark.sql").option("es.resource", '%s' % ('asx_capitalraises_tier2')).option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "changeme").option("es.net.ssl", "true").option("es.nodes","http://54.252.174.27").option("es.port", "9200").option("es.nodes.wan.only","true").mode("overwrite").save()
# with DAG('MakCorp_CapitalRaise_data_dag', description='CapitalRaise DAG', start_date=datetime(2018, 11, 1), catchup=False) as dag:
#     start= DummyOperator(task_id='CapitalRaise_Data_Loading_Started')
#     capitalraises_task	= PythonOperator(task_id='ASX_Capitalraises', python_callable=capitalraises_data)
#     end= DummyOperator(task_id='CapitalRaise_Data_Loading_Completed')
#     start >> capitalraises_task >>end 