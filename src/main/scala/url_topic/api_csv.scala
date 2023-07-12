package url_topic
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import requests._


object api_csv {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MyApp")
      .master("local[*]") // Use all available cores on the local machine
      .getOrCreate()
    val topic = "kajal"
    val brokers = "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-13-101.eu-west-2.compute.internal:9092, ip-172-31-9-237.eu-west-2.compute.internal:9092"
    val url = "http://3.9.191.104:7071/api"

    import spark.implicits._


    val response = get(url)
    val total = response.text()
    val dfFromText = spark.read.json(Seq(total).toDS)
    val kafka_msg = dfFromText.select(to_json(struct($"response"))).toDF("value")
    kafka_msg.selectExpr("value").write.format("kafka").option("kafka.bootstrap.servers", brokers).option("topic", topic).save()
  }



}

