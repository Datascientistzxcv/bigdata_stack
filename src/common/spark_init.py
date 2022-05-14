import logging
from pyspark.sql import SparkSession
from src.common.constants import Constants
from src.common.utility import get_config_value
import os


class SparkInit:
    '''
    Spark initialization and Hudi write options
    '''

    def __init__(self):
        if os.environ.get('APP_SETTINGS') in Constants.DEPLOY_ENV:

            self.spark = SparkSession\
                .builder \
                .config('spark.serializer',
                        'org.apache.spark.serializer.KryoSerializer') \
                .getOrCreate()
        else:
            jars = [
                get_config_value('spark_jars', 'kafka_client'),
                get_config_value('spark_jars', 'kafka_sql'),
                get_config_value('spark_jars', 'kafka_token_provider'),
                get_config_value('spark_jars', 'common_pool')
            ]
            self.spark = SparkSession\
                .builder \
                .config('spark.serializer',
                        'org.apache.spark.serializer.KryoSerializer') \
                .config('spark.jars',
                        ','.join(jars)) \
                .getOrCreate()
        self.logger = self.set_up_logger(self.spark)

    # reuse Spark logger and display logs with WARN level so that they will show up
    def set_up_logger(self, spark):
        logging.basicConfig(level=logging.WARN,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        spark_logger = spark._jvm.org.apache.log4j.Logger
        return spark_logger.getLogger(__name__)