from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import *
from pyspark.sql.functions import udf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
import json

from src.common.spark_init import SparkInit

class KafkaParserRunner():
    '''
    Initalize and run email parser
    '''

    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger
        schema_dict={'fields': [{'metadata': {},
            'name': 'AgentOfLeft',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'AgentOfLeftFFTT',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'AgentOfLeftFFTTId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'AgentOfLeftId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'AgentOfLeftRole',
            'nullable': True,
            'type': 'string'},
            {'metadata': {}, 'name': 'AgentOfPE', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'AgentOfRight', 'nullable': True, 'type': 'string'},
            {'metadata': {},
            'name': 'AgentOfRightFFTT',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'AgentOfRightFFTTId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'AgentOfRightId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'AgentOfRightRole',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'AttributionId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {}, 'name': 'BeginTime', 'nullable': True, 'type': 'string'},
            {'metadata': {},
            'name': 'ClientExtendedPath',
            'nullable': True,
            'type': 'string'},
            {'metadata': {}, 'name': 'ClientId', 'nullable': True, 'type': 'string'},
            {'metadata': {},
            'name': 'ClientParentId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {}, 'name': 'ClientRootId', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'ContainerId', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'CreationTime', 'nullable': True, 'type': 'string'},
            {'metadata': {},
            'name': 'CredentialLeft',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'CredentialLeftFFTT',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'CredentialLeftFFTTId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'CredentialLeftId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'CredentialLeftRole',
            'nullable': True,
            'type': 'string'},
            {'metadata': {}, 'name': 'CredentialPE', 'nullable': True, 'type': 'string'},
            {'metadata': {},
            'name': 'CredentialRight',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'CredentialRightFFTT',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'CredentialRightFFTTId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'CredentialRightId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'CredentialRightRole',
            'nullable': True,
            'type': 'string'},
            {'metadata': {}, 'name': 'EventLabel', 'nullable': True, 'type': 'string'},
            {'metadata': {},
            'name': 'EventTemplate',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'EventTemplateFFTTId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'EventTemplateMATId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {}, 'name': 'GatewayLeft', 'nullable': True, 'type': 'string'},
            {'metadata': {},
            'name': 'GatewayLeftFFTT',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'GatewayLeftFFTTId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'GatewayLeftId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'GatewayLeftRole',
            'nullable': True,
            'type': 'string'},
            {'metadata': {}, 'name': 'GatewayPE', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'GatewayRight', 'nullable': True, 'type': 'string'},
            {'metadata': {},
            'name': 'GatewayRightFFTT',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'GatewayRightFFTTId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'GatewayRightId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'GatewayRightRole',
            'nullable': True,
            'type': 'string'},
            {'metadata': {}, 'name': 'Lat', 'nullable': True, 'type': 'double'},
            {'metadata': {}, 'name': 'Long', 'nullable': True, 'type': 'double'},
            {'metadata': {}, 'name': 'MeetingId', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'MeetingPE', 'nullable': True, 'type': 'string'},
            {'metadata': {},
            'name': 'MeetingParentId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'MeetingParentRole',
            'nullable': True,
            'type': 'string'},
            {'metadata': {}, 'name': 'MeetingRole', 'nullable': True, 'type': 'string'},
            {'metadata': {},
            'name': 'MeetingRootId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {}, 'name': 'PK', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'Registrant', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'SegmentId', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'SegmentJSON', 'nullable': True, 'type': 'string'},
            {'metadata': {},
            'name': 'SegmentParentId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'SegmentRootId',
            'nullable': True,
            'type': 'string'},
            {'metadata': {},
            'name': 'SegmentTemplate',
            'nullable': True,
            'type': 'string'},
            {'metadata': {}, 'name': 'SessionId', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'UpdateTime', 'nullable': True, 'type': 'string'}],
            'type': 'struct'}

        self.new_schema = StructType.fromJson(json.loads(json.dumps(schema_dict)))


    def foreach_batch_function(df, epoch_id):
        df.write.jdbc(url='jdbc:mysql://45.34.23.218:3306/social?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',  table="sparkkafka",  mode="append", properties=db_target_properties)
        pass

    def read_from_kafka(self, topic_input):
        options_read = {
            "kafka.bootstrap.servers":
                "redpandaelb-9807b69466e6f9b2.elb.us-east-1.amazonaws.com:9092",
            "subscribePattern":
                f"{topic_input}.*",
            "startingOffsets":
                "earliest"
        }


        df_input_strings = self.spark.readStream \
            .format("kafka") \
            .options(**options_read) \
            .load()
        ds = df_input_strings.selectExpr(("CAST(value AS STRING)")).select("value",from_json("value", self.new_schema).alias("json")).select("json.*")
        ds.writeStream.outputMode("append").foreachBatch(foreach_batch_function).start()
        self.spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    try:
        spark_init = SparkInit()
        runner = KafkaParserRunner(spark_init.spark, spark_init.logger)
        runner.read_from_kafka("_FFFFFFFFFFFFFF")
    except Exception as exception:
        spark_init.logger.error(str(exception))
        raise exception