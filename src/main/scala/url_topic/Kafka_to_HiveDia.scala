package url_topic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object Kafka_to_HiveDia
{
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
    val topic = "test_kj"

    //$"Age", $"BMI", $"BloodGlucose_Level", $"Diabetes", $"Gender", $"HbA1c_Level", $"Heart_Disease", $"Hypertension",$"ID", $"Name", $"Smoking_History"

    // Define the schema for the JSON messages
    val schema = StructType(Seq(
      StructField("Age", StringType, nullable = true),
      StructField("BMI", StringType, nullable = true),
      StructField("BloodGlucose_Level", StringType, nullable = true),
      StructField("Diabetes", StringType, nullable = true),
      StructField("Gender", StringType, nullable = true),
      StructField("HbA1c_Level", StringType, nullable = true),
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
      .selectExpr("data.*")

    // Write the DataFrame to a Hive external table
    df.writeStream
      .format("hive")
      .option("checkpointLocation", "/tmp/jenkins/kafka/heal/checkpoint")
      .option("path", "/tmp/jenkins/kafka/heal/data")
      .option("table", "health") // Specify the name of your Hive external table
      .option("metastore.catalog.default", "hive") // Specify the catalog to use
      .start()
      .awaitTermination()
  }


}
//sudo -u hdfs hdfs dfs -chmod 777 /tmp/jenkins/kafka/heal/
// hdfs dfs -ls /tmp/jenkins/kafka/heal/