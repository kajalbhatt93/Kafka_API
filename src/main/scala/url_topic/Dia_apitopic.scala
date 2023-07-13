package url_topic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import requests._
object Dia_apitopic {
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

      val messageDF = dfFromText.select($"Age", $"BMI", $"BloodGlucose_Level", $"Diabetes", $"Gender", $"HbA1c_Level", $"Heart_Disease", $"Hypertension", $"ID", $"Name", $"Smoking_History")
      messageDF.show(10)

      val kafkaServer: String = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
      val topicSampleName: String = "daibetics"

      messageDF.selectExpr("CAST(ID AS STRING) AS key", "to_json(struct(*)) AS value").selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format("kafka").option("kafka.bootstrap.servers", kafkaServer).option("topic", topicSampleName).save()

      Thread.sleep(10000) // wait for 10 seconds before making the next call
    }
  }

}
