package url_topic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Dai_typcast {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("KafkaToJson")
      .master("local[*]")
      .enableHiveSupport() // Enable Hive support
      .getOrCreate()

    // Define the Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ip-172-31-3-80.eu-west-2.compute.internal:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "group1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Define the Kafka topic to subscribe to
    val topic = "daibetics"

    // Define the schema for the JSON messages
    val schema = StructType(Seq(
      StructField("Age", FloatType, nullable = true),
      StructField("BMI", FloatType, nullable = true),
      StructField("BloodGlucose_Level", StringType, nullable = true),
      StructField("Gender", StringType, nullable = true),
      StructField("HbA1c_Level", FloatType, nullable = true),
      StructField("Heart_Disease", StringType, nullable = true),
      StructField("Hypertension", StringType, nullable = true),
      StructField("ID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Smoking_History", StringType, nullable = true)
    ))

    // Read the JSON messages from Kafka as a DataFrame
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ip-172-31-3-80.eu-west-2.compute.internal:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").cast("string"), schema).as("data"))
      .selectExpr("CAST(data.Age AS INT) AS AgeInt", "CAST(data.BMI AS INT) AS BMIInt", "data.BloodGlucose_Level", "data.Gender", "CAST(data.HbA1c_Level AS INT) AS HbA1c_LevelInt", "data.Heart_Disease", "data.Hypertension", "data.ID", "data.Name", "data.Smoking_History")

    // Write the DataFrame as CSV files to HDFS
    df.writeStream
      .format("csv")
      .option("checkpointLocation", "/tmp/jenkins/kafka/graph/checkpoint")
      .option("path", "/tmp/jenkins/kafka/graph/data")
      .start()
      .awaitTermination()
  }
}
// sudo -u hdfs hdfs dfs -chmod 777 /tmp/jenkins/kafka/graph
//sudo -u hdfs hdfs dfs -rm -r /tmp/jenkins/kafka/graph/*
//sudo -u hdfs hdfs dfs -chmod 777 /tmp/jenkins/kafka/graph/*
// hdfs dfs -ls /tmp/jenkins/kafka/heal/
