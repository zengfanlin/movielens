import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka010.KafkaUtils
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
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node22:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("movielens").toSet
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    var ss = stream.map(record => (record.key, record.value))
    ss.foreachRDD(rdd => {
      rdd.foreach(line => {
        println(">>>>>>>>>>>>>>>>>>")
        println("key=" + line._1 + "  value=" + line._2)
      })
    })
    streamingContext.start() //spark stream系统启动
    streamingContext.awaitTermination() //
  }
}
