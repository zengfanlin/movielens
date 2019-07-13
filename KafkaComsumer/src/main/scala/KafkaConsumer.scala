
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
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
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import redis.clients.jedis.JedisPool


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
    var sum: Double = 0
    val topics = Array("movielens").toSet
    val stream = KafkaUtils.createDirectStream[String, String](
      scc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //    val sensorDStream = stream.map(_.value()).map(Sensor.parseSensor)
    //5、获取topic中的数据
    var lines = stream.map(_.value())
    val wordCounts = lines.map(x => {
      var values = x.split("\t")
      ("sum", values(3).toDouble)
    }).reduceByKey(_ + _)


    wordCounts.print()
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    stream.foreachRDD(rdd => {
      val offsetRangers = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(p => {
        p.foreach(x => {
          var values = x.value().split("\t")

          /**
            * Internal Redis client for managing Redis connection {@link Jedis} based on {@link RedisPool}
            */
          object InternalRedisClient extends Serializable {
            @transient private var pool: JedisPool = null

            def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
                         maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
              makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)
            }

            def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
                         maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean,
                         testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
              if (pool == null) {
                val poolConfig = new GenericObjectPoolConfig()
                poolConfig.setMaxTotal(maxTotal)
                poolConfig.setMaxIdle(maxIdle)
                poolConfig.setMinIdle(minIdle)
                poolConfig.setTestOnBorrow(testOnBorrow)
                poolConfig.setTestOnReturn(testOnReturn)
                poolConfig.setMaxWaitMillis(maxWaitMillis)
                pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

                val hook = new Thread {
                  override def run = pool.destroy()
                }
                sys.addShutdownHook(hook.run)
              }
            }

            def getPool: JedisPool = {
              assert(pool != null)
              pool
            }
          }
          // Redis configurations
          val maxTotal = 10
          val maxIdle = 10
          val minIdle = 1
          val redisHost = "nodemysql"
          val redisPort = 6379
          val redisTimeout = 30000
          val dbIndex = 1
          val clickHashKey = "movielens::udata::rating::total"
          val sumfield = "sum"

          InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)

          val jedis = InternalRedisClient.getPool.getResource
          jedis.select(dbIndex)

          //原子操作--Redis HINCRBY命令用于增加存储在字段中存储由增量键哈希的数量。
          //如果键不存在,新的key被哈希创建。如果字段不存在,值被设置为0之前进行操作。
          jedis.hincrBy(clickHashKey, sumfield, values(3).toLong)
          InternalRedisClient.getPool.returnResource(jedis)
        })
      })
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRangers)
    })

    //    val kafkaData: DStream[String] = stream.map(_.value())
    //    kafkaData.foreachRDD(rdd => {
    //        rdd.foreachPartition(p => {
    //          var maps = p.map(x => {
    //            var values = x.split("\t")
    //            ("sum",values(3).toDouble)
    //          })
    //          sum = maps.re
    //        })
    //        println(sum)
    //      })

    //    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    //    stream.foreachRDD(rdd => {
    //      //每隔设置的时间会执行一次
    //      val offsetRangers = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //      rdd.foreachPartition(p => {
    //        import java.util
    //        val puts = new util.ArrayList[Put]
    //
    //        p.foreach(record => {
    //          val values = record.value().split("\t")
    //          val put = HbaseUtils.createPut(values(0))
    //          HbaseUtils.addValueOnPut(put, cf1, "userid", values(1))
    //          HbaseUtils.addValueOnPut(put, cf1, "itemid", values(2))
    //          HbaseUtils.addValueOnPut(put, cf1, "rating", values(3))
    //          HbaseUtils.addValueOnPut(put, cf1, "timestamp", values(4))
    //          puts.add(put)
    //        })
    //        if (puts.size() > 0) {
    //          var conn = HbaseUtils.getHbaseConn
    //          HbaseUtils.put(conn, tableName, puts)
    //          conn.close()
    //          println("connectting close:" + conn.hashCode())
    //        }
    //
    //      })
    //      //手动提交offset，保存到kafka
    //      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRangers)
    //    })
    scc.start() //spark stream系统启动
    scc.awaitTermination() //

  }
}

