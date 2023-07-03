package url_topic
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import requests._

object API_to_Kafkatopic {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark Application")
      .master("local[*]")
      .getOrCreate()
    while (true) {
      import spark.implicits._
      val apiUrl = "http://3.9.191.104:7071/api"
      val response = get(apiUrl, headers = headers)
      val total = response.text()
      val dfFromText = spark.read.json(Seq(total).toDS)

      // select the columns you want to include in the message

      val messageDF = dfFromText.select($"DOB", $"ID", $"dept", $"email",$"gender", $"jobtitle", $"name", $"salary")

      val kafkaServer: String = "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-13-101.eu-west-2.compute.internal:9092, ip-172-31-9-237.eu-west-2.compute.internal:9092"
      val topicSampleName: String = "kajal"

      messageDF.selectExpr("CAST(ID AS STRING) AS key", "to_json(struct(*)) AS value").selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format("kafka").option("kafka.bootstrap.servers", kafkaServer).option("topic", topicSampleName).save()

      Thread.sleep(10000) // wait for 10 seconds before making the next call
    }
  }

}

/*DOB": "12/4/2003",
"ID": 1,
"dept": "Purchasing",
"email": "Dakota_Simmons7857@iscmr.club",
"gender": "Female",
"jobtitle": "Project Manager",
"name": "Dakota Simmons",
"salary": 908526*/
