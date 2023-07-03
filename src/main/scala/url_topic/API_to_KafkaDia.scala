package url_topic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import requests._

object API_to_KafkaDia
{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark Application")
      .master("local[*]")
      .getOrCreate()

    while (true) {
      import spark.implicits._
      val apiUrl = "http://127.0.0.1:7071/api"
      val response = get(apiUrl, headers = headers)
      val total = response.text()
      val dfFromText = spark.read.json(Seq(total).toDS)

      // select the columns you want to include in the message

      val messageDF = dfFromText.select($"Age", $"BMI", $"BloodGlucose_Level", $"Diabetes", $"Gender", $"HbA1c_Level", $"Heart_Disease", $"Hypertension",$"ID", $"Name", $"Smoking_History")

      val kafkaServer: String = "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-13-101.eu-west-2.compute.internal:9092, ip-172-31-9-237.eu-west-2.compute.internal:9092"
      val topicSampleName: String = "test_kj"

      messageDF.selectExpr("CAST(ID AS STRING) AS key", "to_json(struct(*)) AS value").selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format("kafka").option("kafka.bootstrap.servers", kafkaServer).option("topic", topicSampleName).save()

      Thread.sleep(10000) // wait for 10 seconds before making the next call
    }
  }

}

// spark-submit --master local[*] --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7","com.lihaoyi:requests_2.11:0.7.1" --class url_topic.API_to_KafkaDia target/Kafka_API-1.0-SNAPSHOT.jar