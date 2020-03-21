package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}

object CsvReadSpark extends App{
  val spark = SparkSession
    .builder
    .appName("Spark-CSV-Read")
    .master("local")
    .getOrCreate()

  val emp_data = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
                  .csv("src\\main\\resources\\emp_data.txt")
  emp_data.show()

}
