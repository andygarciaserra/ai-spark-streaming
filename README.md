# Detección de anomalías en facturas mediante Spark Streaming

**Máster Universitario en Investigación en Inteligencia Artificial (UIMP)**
**Práctica de Big Data – Spark Streaming**

---

# Descripción

Este proyecto implementa una pipeline de procesamiento en tiempo real utilizando **Apache Spark Streaming** y **Apache Kafka** sobre el conjunto de datos **Online Retail**.

La aplicación consume compras individuales desde Kafka, reconstruye las facturas a partir de sus líneas, extrae las características de cada factura y detecta posibles anomalías utilizando dos modelos de clustering ejecutados en paralelo:

* **KMeans**
* **Bisecting KMeans**

Además, durante el procesamiento también:

* identifica compras inválidas;
* contabiliza las facturas canceladas en una ventana deslizante de 8 minutos;
* publica todos los resultados en distintos topics de Kafka.

---

# Estructura del proyecto

```
.
├── build.sbt
├── env.sh
├── start_training.sh
├── start_pipeline.sh
├── productiondata.sh
├── src/
│   └── main/scala/
│       └── es/dmr/uimp/
│           ├── clustering/
│           ├── realtime/
│           └── simulation/
```

Los siguientes directorios y archivos se generan automáticamente durante la ejecución y no forman parte del repositorio:

```
target/
checkpoint/
spark-warehouse/
clustering/
clustering_bisect/
threshold
threshold_bisect
```

---

# Requisitos

El proyecto se ha desarrollado utilizando:

* Java 8
* Scala 2.11
* Apache Spark 2.3.1
* Apache Kafka 0.8.2.1
* sbt

Antes de compilar o ejecutar el proyecto es necesario cargar las variables de entorno:

```bash
source ./env.sh
```

---

# Compilación

Para generar el archivo ejecutable:

```bash
source ./env.sh
sbt clean assembly
```

El resultado será:

```
target/scala-2.11/anomalyDetection-assembly-1.0.jar
```

---

# Entrenamiento de los modelos

El entrenamiento de ambos modelos se realiza mediante:

```bash
./start_training.sh
```

Este script:

* entrena el modelo **KMeans**;
* entrena el modelo **Bisecting KMeans**;
* guarda ambos modelos en disco;
* calcula y almacena los umbrales de detección de anomalías.

Al finalizar se generan:

```
clustering/
threshold

clustering_bisect/
threshold_bisect
```

---

# Ejecución de la pipeline

Una vez iniciado Kafka y Zookeeper, la aplicación puede ejecutarse mediante:

```bash
./start_pipeline.sh
```

En otra terminal se inicia el simulador del flujo de compras:

```bash
./productiondata.sh
```

---

# Topics de Kafka

## Entrada

```
purchases
```

## Salida

```
facturas_erroneas
cancelaciones
anomalias_kmeans
anomalias_bisect_kmeans
```

---

# Arquitectura de la solución

```
production.csv
        │
        ▼
InvoiceDataProducer
        │
        ▼
Kafka (purchases)
        │
        ▼
InvoicePipeline
        │
        ├── Facturas erróneas
        ├── Cancelaciones
        ├── Reconstrucción de facturas
        ├── Detección mediante KMeans
        └── Detección mediante Bisecting KMeans
                │
                ▼
Topics de salida de Kafka
```

---

# Comprobación del funcionamiento

El contenido de los topics puede consultarse utilizando el consumidor de Kafka.

Por ejemplo:

```bash
kafka-console-consumer.sh \
  --zookeeper localhost:2181 \
  --topic anomalias_kmeans \
  --from-beginning
```

Del mismo modo pueden comprobarse los topics:

* `facturas_erroneas`
* `cancelaciones`
* `anomalias_bisect_kmeans`

---

# Notas

* Los modelos de entrenamiento se generan automáticamente mediante `start_training.sh`.
* Los modelos entrenados, checkpoints y demás archivos temporales no forman parte del repositorio y pueden regenerarse en cualquier momento.
* La implementación mantiene la estructura original del proyecto proporcionado, simplificando el código y los scripts para facilitar su comprensión y ejecución.

