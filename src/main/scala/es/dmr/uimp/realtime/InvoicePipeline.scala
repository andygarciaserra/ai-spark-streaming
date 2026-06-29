package es.dmr.uimp.realtime

import java.util.HashMap

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import es.dmr.uimp.clustering.KMeansClusterInvoices
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering.{BisectingKMeansModel, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

object InvoicePipeline {

  // Topics de salida utilizados por la pipeline.
  val InvalidPurchasesTopic = "facturas_erroneas"
  val CancellationsTopic = "cancelaciones"
  val KMeansAnomaliesTopic = "anomalias_kmeans"
  val BisectingKMeansAnomaliesTopic = "anomalias_bisect_kmeans"

  case class Purchase(
    invoiceNo: String,
    quantity: Int,
    invoiceDate: String,
    unitPrice: Double,
    customerID: String,
    country: String
  )

  case class Invoice(
    invoiceNo: String,
    avgUnitPrice: Double,
    minUnitPrice: Double,
    maxUnitPrice: Double,
    time: Double,
    numberItems: Double,
    lastUpdated: Long,
    lines: Int,
    customerId: String
  )

  // Intenta convertir una línea CSV recibida desde Kafka en una compra.
  // Si la línea está incompleta o contiene valores no válidos, se descarta.
  def parsePurchase(line: String): Option[Purchase] = {
    try {
      val parserSettings = new CsvParserSettings()
      parserSettings.getFormat.setLineSeparator("\n")

      val parser = new CsvParser(parserSettings)
      val fields = parser.parseLine(line)

      if (fields == null || fields.length < 8) {
        None
      } else {
        Some(
          Purchase(
            invoiceNo = fields(0),
            quantity = fields(3).toInt,
            invoiceDate = fields(4),
            unitPrice = fields(5).toDouble,
            customerID = fields(6),
            country = fields(7)
          )
        )
      }
    } catch {
      case _: Exception => None
    }
  }

  // Consideramos inválidas las compras sin identificador de factura,
  // sin fecha o sin identificador de cliente.
  def isInvalidPurchase(p: Purchase): Boolean = {
    StringUtils.isEmpty(p.invoiceNo) ||
      StringUtils.isEmpty(p.invoiceDate) ||
      StringUtils.isEmpty(p.customerID)
  }

  // Extrae la hora de la fecha de la factura.
  // Si el formato no es el esperado, se devuelve -1 para evitar romper la pipeline.
  def getHour(invoiceDate: String): Double = {
    try {
      invoiceDate.substring(10).split(":")(0).trim.toDouble
    } catch {
      case _: Exception => -1.0
    }
  }

  // Agrupa las líneas de una misma factura y calcula las características
  // que después se utilizarán para la detección de anomalías.
  def purchasesToInvoice(invoiceNo: String, purchases: Iterable[Purchase]): Invoice = {
    val list = purchases.toList
    val prices = list.map(_.unitPrice)
    val firstPurchase = list.head

    Invoice(
      invoiceNo = invoiceNo,
      avgUnitPrice = prices.sum / prices.size,
      minUnitPrice = prices.min,
      maxUnitPrice = prices.max,
      time = getHour(firstPurchase.invoiceDate),
      numberItems = list.size.toDouble,
      lastUpdated = System.currentTimeMillis(),
      lines = list.size,
      customerId = firstPurchase.customerID
    )
  }

  // Convierte una factura en el vector numérico usado por los modelos.
  def invoiceToVector(invoice: Invoice): Vector = {
    Vectors.dense(
      invoice.avgUnitPrice,
      invoice.minUnitPrice,
      invoice.maxUnitPrice,
      invoice.time,
      invoice.numberItems
    )
  }

  def loadKMeansAndThreshold(
    sc: SparkContext,
    modelFile: String,
    thresholdFile: String
  ): (KMeansModel, Double) = {
    val model = KMeansModel.load(sc, modelFile)
    val threshold = sc.textFile(thresholdFile, 20).map(_.toDouble).first()
    (model, threshold)
  }

  def loadBisectingKMeansAndThreshold(
    sc: SparkContext,
    modelFile: String,
    thresholdFile: String
  ): (BisectingKMeansModel, Double) = {
    val model = BisectingKMeansModel.load(sc, modelFile)
    val threshold = sc.textFile(thresholdFile, 20).map(_.toDouble).first()
    (model, threshold)
  }

  // Calcula la distancia de una factura al centroide asignado por Bisecting KMeans.
  def distToCentroidBisect(datum: Vector, model: BisectingKMeansModel): Double = {
    val centroid = model.clusterCenters(model.predict(datum))
    Vectors.sqdist(datum, centroid)
  }

  def kafkaConf(brokers: String): HashMap[String, Object] = {
    val props = new HashMap[String, Object]()

    props.put(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      brokers
    )

    props.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )

    props.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )

    props
  }

  // Publica los pares clave-valor de un RDD en el topic indicado.
  // Se crea un productor por partición para evitar crear uno por cada mensaje.
  def publishToKafka(
    topic: String
  )(
    kafkaBrokers: Broadcast[String]
  )(
    rdd: RDD[(String, String)]
  ): Unit = {
    rdd.foreachPartition { partition =>
      val producer = new KafkaProducer[String, String](kafkaConf(kafkaBrokers.value))

      partition.foreach { case (key, value) =>
        producer.send(new ProducerRecord[String, String](topic, key, value))
      }

      producer.close()
    }
  }

  def connectToPurchases(
    ssc: StreamingContext,
    zkQuorum: String,
    group: String,
    topics: String,
    numThreads: String
  ): DStream[(String, String)] = {
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 9) {
      System.err.println(
        """
          |Usage:
          |InvoicePipeline <modelFile> <thresholdFile> <modelFileBisect> <thresholdFileBisect>
          |                <zkQuorum> <group> <topics> <numThreads> <brokers>
        """.stripMargin
      )
      System.exit(1)
    }

    val Array(
      modelFile,
      thresholdFile,
      modelFileBisect,
      thresholdFileBisect,
      zkQuorum,
      group,
      topics,
      numThreads,
      brokers
    ) = args

    val sparkConf = new SparkConf().setAppName("InvoicePipeline")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(20))

    // Necesario para las operaciones basadas en ventanas temporales.
    ssc.checkpoint("./checkpoint")

    val (kmeansModel, kmeansThreshold) =
      loadKMeansAndThreshold(sc, modelFile, thresholdFile)

    val (bisectModel, bisectThreshold) =
      loadBisectingKMeansAndThreshold(sc, modelFileBisect, thresholdFileBisect)

    val kmeansModelBc = sc.broadcast(kmeansModel)
    val kmeansThresholdBc = sc.broadcast(kmeansThreshold)
    val bisectModelBc = sc.broadcast(bisectModel)
    val bisectThresholdBc = sc.broadcast(bisectThreshold)
    val kafkaBrokersBc = sc.broadcast(brokers)

    // Nos conectamos al topic de Kafka desde el que llegan las compras.
    val purchasesFeed =
      connectToPurchases(ssc, zkQuorum, group, topics, numThreads)

    val purchases = purchasesFeed.flatMap { case (_, line) =>
      parsePurchase(line)
    }

    // Separamos las compras válidas de las que no tienen la información mínima.
    val invalidPurchases = purchases.filter(isInvalidPurchase)
    val validPurchases = purchases.filter(p => !isInvalidPurchase(p))

    invalidPurchases.foreachRDD { rdd =>
      println("========= INVALID PURCHASES = " + rdd.count() + " =========")
    }

    validPurchases.foreachRDD { rdd =>
      println("========= VALID PURCHASES = " + rdd.count() + " =========")
    }

    invalidPurchases
      .map(p => (p.invoiceNo, p.toString))
      .foreachRDD { rdd =>
        publishToKafka(InvalidPurchasesTopic)(kafkaBrokersBc)(rdd)
      }

    // Las cancelaciones se identifican porque el número de factura empieza por C.
    // Se cuentan en una ventana de 8 minutos que se actualiza cada minuto.
    val cancellations = purchases
      .filter(p => !StringUtils.isEmpty(p.invoiceNo) && p.invoiceNo.startsWith("C"))
      .map(_.invoiceNo)
      .transform(rdd => rdd.distinct())

    val cancellationsWindow = cancellations.window(Minutes(8), Minutes(1))

    cancellationsWindow.foreachRDD { rdd =>
      val count = rdd.distinct().count()

      println("========= CANCELACIONES LAST 8 MIN = " + count + " =========")

      val output = rdd.sparkContext.parallelize(
        Seq(("cancelaciones", count.toString))
      )

      publishToKafka(CancellationsTopic)(kafkaBrokersBc)(output)
    }

    // Reconstruimos las facturas agrupando todas sus líneas de compra.
    val invoices = validPurchases
      .filter(p => !p.invoiceNo.startsWith("C"))
      .map(p => (p.invoiceNo, p))
      .groupByKey()
      .map { case (invoiceNo, invoicePurchases) =>
        purchasesToInvoice(invoiceNo, invoicePurchases)
      }

    invoices.foreachRDD { rdd =>
      println("========= INVOICES BUILT = " + rdd.count() + " =========")
    }

    // Una factura se marca como anómala si supera el umbral aprendido durante el entrenamiento.
    val anomaliesKMeans = invoices.flatMap { invoice =>
      val vector = invoiceToVector(invoice)
      val distance =
        KMeansClusterInvoices.distToCentroid(vector, kmeansModelBc.value)

      if (distance > kmeansThresholdBc.value) {
        Some((invoice.invoiceNo, invoice.toString + ", distance=" + distance))
      } else {
        None
      }
    }

    anomaliesKMeans.foreachRDD { rdd =>
      println("========= ANOMALIES KMEANS = " + rdd.count() + " =========")
      publishToKafka(KMeansAnomaliesTopic)(kafkaBrokersBc)(rdd)
    }

    // Repetimos la detección utilizando el modelo Bisecting KMeans.
    val anomaliesBisect = invoices.flatMap { invoice =>
      val vector = invoiceToVector(invoice)
      val distance = distToCentroidBisect(vector, bisectModelBc.value)

      if (distance > bisectThresholdBc.value) {
        Some((invoice.invoiceNo, invoice.toString + ", distance=" + distance))
      } else {
        None
      }
    }

    anomaliesBisect.foreachRDD { rdd =>
      println("========= ANOMALIES BISECT = " + rdd.count() + " =========")
      publishToKafka(BisectingKMeansAnomaliesTopic)(kafkaBrokersBc)(rdd)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
