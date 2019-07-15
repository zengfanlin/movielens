
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.JedisPool


object KafkaConsumerTest extends Serializable {


  val config = Map(
    "spark.cores" -> "local[*]",
    "kafka.topic" -> "movielens"
  )
  val tableName = "movielens:udata"
  val cf1 = "cf1"
  val columns = Array("userid", "itemid", "rating", "timestamp")

  def main(args: Array[String]): Unit = {
    Streaminglogs.setStreamingLogLevels()
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")

    val spark:SparkSession = SparkSession.builder().appName("appName").getOrCreate()
    var df:Dataset[Row] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node22:9092")
      .option("subscribe", "movielens")
      .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")




  }
}

