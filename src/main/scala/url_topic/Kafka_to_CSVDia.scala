package url_topic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object Kafka_to_CSVDia
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
    val topic = "daibetics"

    //ID	Name	Gender	Age	Hypertension	Heart_Disease	Smoking_History	BMI	HbA1c_Level	BloodGlucose_Level	Diabetes
  //{"ID":60001,"Name":"Luna Milner","Age":62.0,"Gender":"Female","Hypertension":0,"Heart_Disease":0,"Smoking_History":"never","BMI":36.39,"HbA1c_Level":6.1,"BloodGlucose_Level":85,"Diabetes":0}
    // Define the schema for the JSON messages
    val schema = StructType(Seq(
      StructField("ID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Age", StringType, nullable = true),
      StructField("Gender", StringType, nullable = true),
      StructField("Hypertension", StringType, nullable = true),
      StructField("Heart_Disease", StringType, nullable = true),
      StructField("Smoking_History", StringType, nullable = true),
      StructField("BMI", StringType, nullable = true),
      StructField("HbA1c_Level", StringType, nullable = true),
      StructField("BloodGlucose_Level", StringType, nullable = true)
    ))
    print(schema)

    // Read the JSON messages from Kafka as a DataFrame
    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "ip-172-31-14-3.eu-west-2.compute.internal:9092").option("subscribe", topic).option("startingOffsets", "earliest").load().select(from_json(col("value").cast("string"), schema).as("data")).selectExpr("data.*")
    df.show(10)
    // Write the DataFrame as CSV files to HDFS
    df.writeStream.format("csv").option("checkpointLocation", "/tmp/jenkins/kafka/emp_data/checkpoint").option("path", "/tmp/jenkins/kafka/emp_data/data").start().awaitTermination()

  }

}
//sudo -u hdfs hdfs dfs -rm -r /tmp/jenkins/kafka/heal/*
//sudo -u hdfs hdfs dfs -chmod 777 /tmp/jenkins/kafka/heal/*
// hdfs dfs -ls /tmp/jenkins/kafka/heal/
/*
use sample;
CREATE EXTERNAL TABLE sample.dai_realtime(Age INT,BMI FLOAT,BloodGlucose_Level INT,Diabetes INT,Gender STRING,HbA1c_Level FLOAT,Heart_Disease INT,Hypertension INT,ID INT,Name STRING,Smoking_History STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/jenkins/kafka/heal/data/part-00000-b664ce45-14cc-4eca-9d88-12885113e55d-c000.csv';
*/
/*
create external table sample.daibetic_hist(Age INT,BMI FLOAT,BloodGlucose_Level INT,Diabetes INT,Gender STRING,HbA1c_Level FLOAT,Heart_Disease INT,Hypertension INT,ID INT,Name STRING,Smoking_History STRING)
row format delimited
fields terminated by ','
stored as textfile
location '/tmp/kajal/postNifi/';
*/
