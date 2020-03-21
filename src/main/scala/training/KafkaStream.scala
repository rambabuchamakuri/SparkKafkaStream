package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}

object KafkaStream extends App{

  val spark = SparkSession
    .builder
    .appName("Spark-Kafka-Integration")
    .master("local")
    .getOrCreate()

  val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "mathb60-kaf01.ag.purdue.edu:6667,mathb60-kaf02.ag.purdue.edu:6667,mathb60-kaf03.ag.purdue.edu:6667")
      .option("subscribe", "training.input")
      .option("startingOffsets", "earliest")
      .load()

  df.printSchema()

  val personStringDF = df.selectExpr("CAST(value AS STRING)")



}
