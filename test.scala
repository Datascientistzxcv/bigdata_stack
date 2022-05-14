import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame

val spark = SparkSession.builder().master("local[*]").getOrCreate()
import spark.implicits._
spark.sparkContext.setLogLevel("ERROR")

def dfOps(ds: DataFrame, n: Long) =
    ds.select(from_json('value,
      schema_of_json(ds.select('value).first().getString(0))).as("json")).select("json.*").show()

  spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "redpandaelb-9807b69466e6f9b2.elb.us-east-1.amazonaws.com:9092")
    .option("subscribe", "_FFFFFFFFFFFFFF00001575592773802029_")
    .option("startingOffsets", "earliest")
    .load()
    .select('value.cast("string"))
    .writeStream
    .foreachBatch(dfOps _)
    .start()
    .awaitTermination()