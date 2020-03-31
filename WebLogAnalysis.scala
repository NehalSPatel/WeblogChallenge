/**
  * Created by cloudera on 3/26/20.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object WebLogAnalysis {

  def main(args: Array[String]) {

    val log = Logger.getLogger(getClass.getName)
    log.setLevel(Level.INFO)

    /* Creating Spark Session */
    val sparkSession = SparkSession
      .builder()
      .appName("Web Log Analysis")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._

    sparkSession.sparkContext.setLogLevel("ERROR")

    println("InputPath" + args(0))

    val InputFile = args(0)

    val schema = StructType(Array(
      StructField("timestamp", StringType),
      StructField("elb", StringType),
      StructField("client_port", StringType),
      StructField("backend_port", StringType),
      StructField("request_processing_time", FloatType),
      StructField("backend_processing_time", FloatType),
      StructField("response_processing_time", FloatType),
      StructField("elb_status_code", IntegerType),
      StructField("backend_status_code", IntegerType),
      StructField("received_bytes", LongType),
      StructField("sent_bytes", LongType),
      StructField("request", StringType),
      StructField("user_agent", StringType),
      StructField("ssl_cipher", StringType),
      StructField("ssl_protocol", StringType)))

    val InputWebSessionLog_df = sparkSession.read.format("csv").schema(schema).option("delimiter", " ").load("hdfs://quickstart.cloudera"+InputFile)

    println("before filter null : " + InputWebSessionLog_df.count())

    val WebSessionLog_df = InputWebSessionLog_df.where(
        trim($"timestamp") =!= "NULL"
          and trim($"timestamp") =!= ""
          and trim($"client_port") =!= ""
          and trim($"client_port") =!= "NULL"
          and trim($"backend_port") =!= ""
          and trim($"backend_port") =!= "NULL"
          and trim($"request_processing_time") =!= ""
          and trim($"request_processing_time") =!= "NULL"
          and trim($"backend_processing_time") =!= ""
          and trim($"backend_processing_time") =!= "NULL"
          and trim($"response_processing_time") =!= ""
          and trim($"response_processing_time") =!= "NULL"
          and trim($"request") =!= ""
          and trim($"request") =!= "NULL")
      .withColumn("client_ip", split(col("client_port"), "\\:").getItem(0))
      .withColumn("client_port", split(col("client_port"), "\\:").getItem(1))
      .withColumn("request_method", split(col("request"), "\\ ").getItem(0))
      .withColumn("request_url", split(col("request"), "\\ ").getItem(1))
      .withColumn("request_protocol", split(col("request"), "\\ ").getItem(2))
      .withColumn("session_duration",($"request_processing_time")+($"backend_processing_time")+($"response_processing_time"))
      .select("timestamp","client_ip","request_processing_time","backend_processing_time","response_processing_time","session_duration","request_method","request_url","request_protocol")

    println("after filter null : " + WebSessionLog_df.count())

    //Here I am assuming session is per record

    println("1.	Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session. https://en.wikipedia.org/wiki/Session_(web_analytics)")
    WebSessionLog_df.groupBy("client_ip").count().show(false)
    WebSessionLog_df.groupBy("client_ip").agg(concat_ws("|", collect_list(col("request_url"))).as("page_hits")).show(false)

    println("2.	Determine the average session time")
    WebSessionLog_df.select(avg("session_duration") as "average_sessiontime").show()

    println("4.	Find the most engaged users, ie the IPs with the longest session times")
    val Max_sessiontime = WebSessionLog_df.select(max("session_duration") as "max_sessiontime").take(1)(0)(0).toString
    WebSessionLog_df.where(s"trim(session_duration) == ${Max_sessiontime}").select("client_ip","session_duration").show()
    WebSessionLog_df.orderBy(desc("session_duration")).show(100,false)

    println("3.	Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.")
    WebSessionLog_df.groupBy("client_ip").agg(count("request_url"),concat_ws("|", collect_set(col("request_url"))).as("page_hits")).show(false)

  }
}
