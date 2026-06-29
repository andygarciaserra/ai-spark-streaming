package es.dmr.uimp.clustering

import es.dmr.uimp.clustering.Clustering.elbowSelection
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KMeansClusterInvoices {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println(
        "Usage: KMeansClusterInvoices <inputFile> <modelOutputDir> <thresholdOutputFile>"
      )
      System.exit(1)
    }

    import Clustering._

    val sparkConf = new SparkConf().setAppName("ClusterInvoices")
    val sc = new SparkContext(sparkConf)

    val df = loadData(sc, args(0))

    // Calculamos las características por factura y eliminamos registros no válidos.
    val featurized = featurizeData(df)
    val filtered = filterData(featurized)

    // Convertimos cada factura en el vector que utilizará MLlib.
    val dataset = toDataset(filtered)

    // El dataset se reutiliza varias veces durante el entrenamiento y el cálculo del umbral.
    dataset.cache()

    val model = trainModel(dataset)
    model.save(sc, args(1))

    // El umbral se toma como la última distancia dentro de las 2000 más lejanas.
    val distances = dataset.map(d => distToCentroid(d, model))
    val threshold = distances.top(2000).last

    saveThreshold(threshold, args(2))

    sc.stop()
  }

  // Entrena varios modelos KMeans y selecciona uno mediante una heurística tipo elbow.
  def trainModel(data: RDD[Vector]): KMeansModel = {
    val models = 1 to 20 map { k =>
      val kmeans = new KMeans()
      kmeans.setK(k)
      kmeans.run(data)
    }

    val costs = models.map(model => model.computeCost(data))
    val selected = elbowSelection(costs, 0.7)

    println("Selecting KMeans model: " + models(selected).k)

    models(selected)
  }

  // Calcula la distancia entre un punto y el centroide del clúster al que pertenece.
  def distToCentroid(datum: Vector, model: KMeansModel): Double = {
    val centroid = model.clusterCenters(model.predict(datum))
    Vectors.sqdist(datum, centroid)
  }
}

object BisectingKMeansClusterInvoices {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println(
        "Usage: BisectingKMeansClusterInvoices <inputFile> <modelOutputDir> <thresholdOutputFile>"
      )
      System.exit(1)
    }

    import Clustering._

    val sparkConf = new SparkConf().setAppName("BisectingClusterInvoices")
    val sc = new SparkContext(sparkConf)

    val df = loadData(sc, args(0))

    // Usamos exactamente las mismas variables que en KMeans para poder comparar resultados.
    val featurized = featurizeData(df)
    val filtered = filterData(featurized)
    val dataset = toDataset(filtered)

    dataset.cache()

    val model = trainModel(dataset)
    model.save(sc, args(1))

    // El criterio de umbral se mantiene igual que en KMeans.
    val distances = dataset.map(d => distToCentroid(d, model))
    val threshold = distances.top(2000).last

    saveThreshold(threshold, args(2))

    sc.stop()
  }

  // Entrena varios modelos Bisecting KMeans y selecciona uno con la misma heurística.
  def trainModel(data: RDD[Vector]): BisectingKMeansModel = {
    val models = 1 to 20 map { k =>
      val bisecting = new BisectingKMeans()
      bisecting.setK(k)
      bisecting.run(data)
    }

    val costs = models.map(model => model.computeCost(data))
    val selected = elbowSelection(costs, 0.7)

    println("Selecting Bisecting KMeans model: " + models(selected).k)

    models(selected)
  }

  // Calcula la distancia entre una factura y el centroide asignado por Bisecting KMeans.
  def distToCentroid(datum: Vector, model: BisectingKMeansModel): Double = {
    val centroid = model.clusterCenters(model.predict(datum))
    Vectors.sqdist(datum, centroid)
  }
}
