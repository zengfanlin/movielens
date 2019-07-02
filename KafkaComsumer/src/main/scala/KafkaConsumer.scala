
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import scala.collection.mutable.ArrayBuffer


object KafkaConsumer {

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
    val scc = new StreamingContext(conf, Seconds(3))
    val sc = scc.sparkContext


    scc.checkpoint("./Kafka_Receiver")
    var zkHost = "node22,node21,node23"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node22:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      //      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("movielens").toSet
    val stream = KafkaUtils.createDirectStream[String, String](
      scc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
   stream.foreachRDD(rdd=>{
     val offsetRangers = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
     rdd.foreachPartition(partitionRecords => {


       partitionRecords.foreach(line => {
         //打印
         println(line.partition() + ":" + line.offset() + ">>>" + line.value())
         val values = line.value().split("\t")
         hbase.putData(tableName,values(0),cf1,"userid",values(1))
         hbase.putData(tableName,values(0),cf1,"itemid",values(2))
         hbase.putData(tableName,values(0),cf1,"rating",values(3))
         hbase.putData(tableName,values(0),cf1,"timestamp",values(4))
       })
     })
     //手动提交offset，保存到kafka
     stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRangers)
   })
    scc.start() //spark stream系统启动
    scc.awaitTermination() //

  }

  def insert_hb(values: Array[String], onePut: Put): Unit = {

    onePut.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("userid"), Bytes.toBytes(values(1)))
    onePut.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("itemid"), Bytes.toBytes(values(2)))
    onePut.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("rating"), Bytes.toBytes(values(3)))
    onePut.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("timestamp"), Bytes.toBytes(values(4)))
  }
}

