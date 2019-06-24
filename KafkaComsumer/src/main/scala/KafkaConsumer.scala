import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object KafkaConsumer {

  val config = Map(
    "spark.cores" -> "local[*]",
    "kafka.topic" -> "movielens"
  )

  def main(args: Array[String]): Unit = {
    var sparkConf: SparkConf = new SparkConf()
      .setAppName("SparkStreamingKafka_Receiver")
      .setMaster(config("spark.cores"))
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")

    //创建Spark的对象
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("./Kafka_Receiver")

    import spark.implicits._
    //创建到Kafka的连接
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node22:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("movielens")
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val ratingStream = kafkaStream.map(record => (record.key, record.value))
    ratingStream.foreachRDD { rdd =>
      rdd.map { case (key, value) =>
        println(">>>>>>>>>>>>>>>>")
        println(value)
      }.count()

      // UID|MID|SCORE|TIMESTAMP
      // 产生评分流
      //    val ratingStream = kafkaStream.map { case msg =>
      //      var attr = msg.value().split("\\|")
      //      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
      //    }
      //

    }
    ssc.start()
    ssc.awaitTermination()
  }
}
