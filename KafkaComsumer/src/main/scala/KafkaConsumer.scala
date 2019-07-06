
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, Put, Result}
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
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.hadoop.mapred.JobConf
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


object KafkaConsumer extends Serializable {

  // schema for sensor data
  case class Sensor(rowkey: String, userid: String, itemid: String, rating: Double, timestamp: String) extends Serializable

  final val cfDataBytes = Bytes.toBytes("cf1")
  final val userid = Bytes.toBytes("userid")
  final val itemid = Bytes.toBytes("itemid")
  final val rating = Bytes.toBytes("rating")
  final val timestamp = Bytes.toBytes("timestamp")

  object Sensor extends Serializable {
    // function to parse line of sensor data into Sensor class

    def parseSensor(str: String): Sensor = {
      val p = str.split("\t")
      Sensor(p(0), p(1), p(2), p(3).toDouble, p(4))
    }

    //  Convert a row of sensor object data to an HBase put object
    def convertToPut(sensor: Sensor): (ImmutableBytesWritable, Put) = {

      val rowkey = sensor.rowkey
      val put = new Put(Bytes.toBytes(rowkey))
      // add to column family data, column  data values to put object
      put.addColumn(cfDataBytes, userid, Bytes.toBytes(sensor.userid))
      put.addColumn(cfDataBytes, itemid, Bytes.toBytes(sensor.itemid))
      put.addColumn(cfDataBytes, rating, Bytes.toBytes(sensor.rating))
      put.addColumn(cfDataBytes, timestamp, Bytes.toBytes(sensor.timestamp))
      return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
    }

  }

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
    val scc = new StreamingContext(conf, Seconds(3)) //6秒一个批次
    val sc = scc.sparkContext
    // set up HBase Table configuration


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
    //    val sensorDStream = stream.map(_.value()).map(Sensor.parseSensor)

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    stream.foreachRDD(rdd => {
      //每隔设置的时间会执行一次
      val offsetRangers = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(p => {
        import java.util
        val puts = new util.ArrayList[Put]
        var conn = HbaseUtils.getHbaseConn
        p.foreach(record => {
          val values = record.value().split("\t")
          import org.apache.hadoop.hbase.client.Put
          val put = HbaseUtils.createPut(values(0))
          HbaseUtils.addValueOnPut(put, cf1, "userid", values(1))
          HbaseUtils.addValueOnPut(put, cf1, "itemid", values(2))
          HbaseUtils.addValueOnPut(put, cf1, "rating", values(3))
          HbaseUtils.addValueOnPut(put, cf1, "timestamp", values(4))
          puts.add(put)
        })
        HbaseUtils.put(conn, tableName, puts)
        conn.close()
        println("connectting close:"+conn.hashCode() )
        //        println(p) //iterator
      })
      //手动提交offset，保存到kafka
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRangers)
    })
    scc.start() //spark stream系统启动
    scc.awaitTermination() //

  }
}

