import java.util.Properties
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}

/**
  * From http://allegro.tech/2015/08/spark-kafka-integration.html
  */
class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {
  lazy val producer = createProducer()

  def send(topic: String, value: String): Unit = producer.send(new ProducerRecord(topic, value))
}

object KafkaSink {
  def apply(config: Properties): KafkaSink = {
    val f = () => {
      val producer = new KafkaProducer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new KafkaSink(f)
  }
}
