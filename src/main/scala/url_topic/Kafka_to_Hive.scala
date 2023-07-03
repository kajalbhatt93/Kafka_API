package url_topic
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object Kafka_to_Hive {
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
      val topic = "kajal"

      // Define the schema for the JSON messages
      val schema = StructType(Seq(
        StructField("DOB", StringType, nullable = true),
        StructField("ID", StringType, nullable = true),
        StructField("dept", StringType, nullable = true),
        StructField("email", StringType, nullable = true),
        StructField("gender", StringType, nullable = true),
        StructField("jobtitle", StringType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("salary", StringType, nullable = true)
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
        .option("checkpointLocation", "/tmp/jenkins/kafka/emp_data/checkpoint")
        .option("path", "/tmp/jenkins/kafka/emp_data/data")
        .option("table", "your_external_table_name") // Specify the name of your Hive external table
        .option("metastore.catalog.default", "hive") // Specify the catalog to use
        .start()
        .awaitTermination()
    }
  }



