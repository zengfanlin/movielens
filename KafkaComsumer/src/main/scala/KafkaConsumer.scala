import java.text.SimpleDateFormat
import java.util
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object KafkaConsumer {

  val config = Map(
    "spark.cores" -> "local[*]",
    "kafka.topic" -> "movielens"
  )

  def main(args: Array[String]): Unit = {
    Streaminglogs.setStreamingLogLevels()
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val streamingContext = new StreamingContext(conf, Seconds(3))
    streamingContext.checkpoint("./Kafka_Receiver")
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
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    //var ss = stream.map(record => (record.key, record.value))
    val tableName = "movielens:udata"
    val cf1 = "cf1"
    val columns = Array("userid", "itemid", "rating", "timestamp")
    stream.foreachRDD(rdd => {

      val offsetRangers = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(partitionRecords => {
        HBaseUtils1x.init()
        partitionRecords.foreach(line => {
          val values = line.value().split("\t")
          val cols = Array(values(1), values(2), values(3), values(4))
          HBaseUtils1x.insertData(tableName, HBaseUtils1x.getPutAction(values(0), cf1, columns, cols))

        })
        HBaseUtils1x.closeConnection()
      })



      //      rdd.foreachPartition(partitionRecords => {
      //        val hbase: HbaseUtils = HbaseUtils.getInstance
      //
      //        partitionRecords.foreach(line => {
      //          //打印
      //          println(line.partition() + ":" + line.offset() + ">>>" + line.value())
      //          val values = line.value().split("\t")
      //          hbase.putData(tableName,values(0),cf1,"userid",values(1))
      //          hbase.putData(tableName,values(0),cf1,"itemid",values(2))
      //          hbase.putData(tableName,values(0),cf1,"rating",values(3))
      //          hbase.putData(tableName,values(0),cf1,"timestamp",values(4))
      //        })
      //      })

      //手动提交offset，保存到kafka
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRangers)
    })
    streamingContext.start() //spark stream系统启动
    streamingContext.awaitTermination() //
    HbaseUtils.close()
  }
}
