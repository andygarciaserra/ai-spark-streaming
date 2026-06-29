package es.dmr.uimp.clustering

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.ArrayBuffer

object Clustering {

  // Carga el CSV original, infiere los tipos de datos y añade una columna
  // con la hora extraída a partir de la fecha de la factura.
  def loadData(sc: SparkContext, file: String): DataFrame = {
    val sqlContext = new SQLContext(sc)

    val getHour = udf[Double, String] { date: String =>
      var hourValue = -1.0

      if (!StringUtils.isEmpty(date)) {
        val hour = date.substring(10).split(":")(0)

        if (!StringUtils.isEmpty(hour)) {
          hourValue = hour.trim.toDouble
        }
      }

      hourValue
    }

    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(file)
      .withColumn("Hour", getHour(col("InvoiceDate")))
  }

  // Agrupa las líneas del dataset por factura y calcula las mismas variables
  // que después utilizará la pipeline en tiempo real.
  def featurizeData(df: DataFrame): DataFrame = {
    df.groupBy("InvoiceNo")
      .agg(
        avg("UnitPrice").alias("AvgUnitPrice"),
        min("UnitPrice").alias("MinUnitPrice"),
        max("UnitPrice").alias("MaxUnitPrice"),
        first("Hour").alias("Time"),
        count("*").alias("NumberItems"),
        first("CustomerID").alias("CustomerID"),
        first("InvoiceDate").alias("InvoiceDate")
      )
  }

  // Elimina facturas incompletas, cancelaciones y registros que no pueden
  // convertirse de forma segura en vectores para el entrenamiento.
  def filterData(df: DataFrame): DataFrame = {
    df.filter(
      col("InvoiceNo").isNotNull &&
        !col("InvoiceNo").startsWith("C") &&
        col("CustomerID").isNotNull &&
        col("InvoiceDate").isNotNull &&
        col("InvoiceDate") =!= "" &&
        col("AvgUnitPrice").isNotNull &&
        col("MinUnitPrice").isNotNull &&
        col("MaxUnitPrice").isNotNull &&
        col("Time") >= 0 &&
        col("NumberItems") > 0
    )
  }

  // Convierte cada factura ya agregada en el vector numérico que reciben
  // los modelos de clustering.
  def toDataset(df: DataFrame): RDD[Vector] = {
    df.select(
        "AvgUnitPrice",
        "MinUnitPrice",
        "MaxUnitPrice",
        "Time",
        "NumberItems"
      )
      .rdd
      .map { row =>
        val buffer = ArrayBuffer[Double]()

        buffer.append(row.getAs[Double]("AvgUnitPrice"))
        buffer.append(row.getAs[Double]("MinUnitPrice"))
        buffer.append(row.getAs[Double]("MaxUnitPrice"))
        buffer.append(row.getAs[Double]("Time"))
        buffer.append(row.getLong(4).toDouble)

        Vectors.dense(buffer.toArray)
      }
  }

  // Selecciona el primer punto en el que la mejora deja de ser suficientemente
  // grande. Si no encuentra un punto claro, se queda con el último modelo probado.
  def elbowSelection(costs: Seq[Double], ratio: Double): Int = {
    for (i <- 1 until costs.length) {
      val currentRatio = costs(i) / costs(i - 1)

      if (currentRatio > ratio) {
        return i
      }
    }

    costs.length - 1
  }

  // Guarda en disco el umbral que se usará después para decidir si una factura
  // debe considerarse anómala.
  def saveThreshold(threshold: Double, fileName: String): Unit = {
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))

    bw.write(threshold.toString)
    bw.close()
  }
}
