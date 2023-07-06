package url_topic
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
object HdfsCSVtoHive
{
  def main(args: Array[String]): Unit =
  {
      // Create SparkSession
      val spark = SparkSession
        .builder()
        .appName("CsvToHive")
        .enableHiveSupport()
        .getOrCreate()

      // Read CSV file into DataFrame
      val csvPath = "hdfs://<hdfs_path_to_csv_file>"
      val csvData: DataFrame = spark.read.format("csv").option("header", "true").load(csvPath)

      // Create a temporary view for the DataFrame
      csvData.createOrReplaceTempView("health")

      // Create an external table in Hive
      val hiveTable = "health"
      val createTableQuery =
        s"""
           |CREATE EXTERNAL TABLE IF NOT EXISTS $hiveTable (
           |  column1 STRING,
           |  column2 INT,
           |  ...
           |)
           |ROW FORMAT DELIMITED
           |FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE
           |LOCATION '$csvPath'
           |""".stripMargin
      spark.sql(createTableQuery)

      // Insert data from the temporary view into the Hive table
      val insertQuery = s"INSERT OVERWRITE TABLE $hiveTable SELECT * FROM temp_table"
      spark.sql(insertQuery)

      // Verify data in the Hive table
      val selectQuery = s"SELECT * FROM $hiveTable"
      val result: DataFrame = spark.sql(selectQuery)
      result.show()

      // Optionally, you can save the DataFrame as a Hive table directly
      // csvData.write.mode(SaveMode.Overwrite).saveAsTable(hiveTable)

      // Stop the SparkSession
      spark.stop()
    }
  }



