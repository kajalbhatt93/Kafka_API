package url_topic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object API_time2
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

    val topic = "aaaatest"
    // Define the schema for the JSON messages
    val schema = StructType(Seq(
      StructField("Age", StringType, nullable = true),
      StructField("BMI", StringType, nullable = true),
      StructField("BloodGlucose_Level", StringType, nullable = true),
      //StructField("Diabetes", StringType, nullable = true),
      StructField("Gender", StringType, nullable = true),
      StructField("HbA1c_Level", StringType, nullable = true),
      StructField("Heart_Disease", StringType, nullable = true),
      StructField("Hypertension", StringType, nullable = true),
      StructField("ID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Smoking_History", StringType, nullable = true)
    ))
    print(schema)


      // Read the JSON messages from Kafka as a DataFrame
      val df = spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "ip-172-31-3-80.eu-west-2.compute.internal:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load().select(from_json(col("value").cast("string"), schema).as("data")).selectExpr("data.*")

      // Write the DataFrame as CSV files to HDFS
      df.writeStream.format("csv")
        .option("checkpointLocation", "/tmp/jenkins/kafka/heal/checkpoint")
        .option("path", "/tmp/jenkins/kafka/heal/data")
        .start()
        .awaitTermination()



  }

}
// hdfs dfs -ls /tmp/jenkins/kafka/heal/
// sudo -u hdfs hdfs dfs -chmod 777 /tmp/jenkins/kafka/heal
//sudo -u hdfs hdfs dfs -rm -r /tmp/jenkins/kafka/heal/*
// sudo -u hdfs hdfs dfs -chmod 777 /tmp/jenkins/kafka/heal/*

/*
use sample;
CREATE EXTERNAL TABLE sample.real_dai1 (
  Age float,
  BMI float,
  BloodGlucose_Level int,
  Gender STRING,
  HbA1c_Level float,
  Heart_Disease int,
  Hypertension int,
  ID int,
  Name STRING,
  Smoking_History STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/jenkins/kafka/heal/data/'
TBLPROPERTIES ("csv.input.fileextension"=".csv");
*/
// crontab -e

//  */5 * * * * impala-shell -i ip-172-31-14-3.eu-west-2.compute.internal -d default -q "invalidate metadata"
