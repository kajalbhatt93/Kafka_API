import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KAAP {
  def main(args: Array[String]): Unit = {
    // Kafka topic details
    val topic = "kajal"
    val bootstrapServers = "ip-172-31-5-217.eu-west-2.compute.internal:9092"

    // Kafka producer properties
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Create a Kafka producer
    val producer = new KafkaProducer[String, String](props)

    // API endpoint URL
    val url = "http://3.9.191.104:7071/api"

    // Read the API response
    val response = scala.io.Source.fromURL(url).mkString

    // Create a Kafka record with the API response as value
    val record = new ProducerRecord[String, String](topic, null, response)

    // Send the record to the Kafka topic
    producer.send(record)

    // Close the Kafka producer
    producer.close()
  }
}
