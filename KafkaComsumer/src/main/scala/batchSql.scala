import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object batchSql {
  def main(args: Array[String]): Unit = {
    Streaminglogs.setStreamingLogLevels()
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
//    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
//    val data = Array((1, "val1"), (2, "val2"), (3, "val3"))
//    var df = spark.createDataFrame(data).toDF("key", "value")
//    df.createOrReplaceTempView("temp_src")
//    sql("insert into src select key,value from temp_src")
    sql("use movielensdb")
    sql("SELECT  avg(rating) FROM udata ").show()

    spark.close()
  }
}
