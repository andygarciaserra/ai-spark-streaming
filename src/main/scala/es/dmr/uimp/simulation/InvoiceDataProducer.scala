package es.dmr.uimp.simulation

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source
import scala.util.Random

object InvoiceDataProducer extends App {

  if (args.length != 3) {
    System.err.println(
      "Usage: InvoiceDataProducer <eventsFile> <topic> <brokers>"
    )
    System.exit(1)
  }

  val eventsFile = args(0)
  val topic = args(1)
  val brokers = args(2)

  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "InvoiceDataProducer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  println("Sending purchases!")

  // Leemos el fichero de producción y enviamos cada línea al topic de Kafka.
  for (line <- Source.fromFile(eventsFile).getLines()) {
    val invoiceNo = line.split(",")(0)
    val data = new ProducerRecord[String, String](topic, invoiceNo, line)

    producer.send(data)

    // Pequeña espera aleatoria para simular una llegada progresiva de compras.
    Thread.sleep(5 + (5 * Random.nextFloat()).toInt)
  }

  producer.close()
}
