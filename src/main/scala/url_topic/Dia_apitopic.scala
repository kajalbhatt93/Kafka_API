package url_topic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import requests._
import scala.concurrent.duration._

object Dia_apitopic {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark Application")
      .master("local[*]")
      .getOrCreate()
    val numIterations = 10

    for (_ <- 1 to numIterations)
    {
      import spark.implicits._

      val apiUrl = "http://3.9.191.104:7071/api"
      val response = get(apiUrl, headers = headers)
      val total = response.text()
      val dfFromText = spark.read.json(Seq(total).toDS)

      //val messageDF = dfFromText.select($"Age", $"BMI", $"BloodGlucose_Level", $"Diabetes", $"Gender", $"HbA1c_Level", $"Heart_Disease", $"Hypertension", $"ID", $"Name", $"Smoking_History")
      val messageDF = dfFromText.select($"Age", $"BMI", $"BloodGlucose_Level", $"Gender", $"HbA1c_Level", $"Heart_Disease", $"Hypertension", $"ID", $"Name", $"Smoking_History")
      messageDF.show(10)

      val kafkaServer: String = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
      val topicSampleName: String = "daibetics"

      messageDF.selectExpr("CAST(ID AS STRING) AS key", "to_json(struct(*)) AS value").selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format("kafka").option("kafka.bootstrap.servers", kafkaServer).option("topic", topicSampleName).save()

      print("######  Sleeping for 1 min ######")
      Thread.sleep(1.minutes.toMillis)
    }
}

}

// kafka-topics --bootstrap-server ip-172-31-14-3.eu-west-2.compute.internal:9092,ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-13-101.eu-west-2.compute.internal:9092, ip-172-31-9-237.eu-west-2.compute.internal:9092 --create --topic daibetics
// sudo su hdfs hdfs dfs -rm -r /tmp/jenkins/kafka/daibetic/*
// sudo -u hdfs hdfs dfs -rm -R /tmp/jenkins/kafka/daibetic/*
// hdfs dfs -ls /tmp/jenkins/kafka/daibetic/
// spark-submit --master local[*] --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7","com.lihaoyi:requests_2.11:0.7.1" --class url_topic.Dia_apitopic target/Kafka_API-1.0-SNAPSHOT.jar