package training

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, to_date, to_timestamp}
import org.apache.spark.sql.Column

object PstlCSVReader extends App{

  val spark = SparkSession
    .builder
    .appName("Spark-CSV-Read")
    .master("local")
    .getOrCreate()

  val emp_data = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    .csv("src\\main\\resources\\CR1000Avg-Input.csv")

  emp_data.show()

  //val emp_data_csv = emp_data.where(emp_data("TIMESTAMP") === "2016-05-25 23:55:00")
  //val emp_data_csv = emp_data.where(emp_data("TIMESTAMP") > "2016-05-25 22:55:00" &&  emp_data("TIMESTAMP") < "2016-05-25 23:55:00")

  //val emp_data_csv = emp_data.where(emp_data("Batt_Volt") === "11.77")

  // Adding new column to dataframe
  val emp_data_csv = emp_data.withColumn("date",to_date(emp_data("TIMESTAMP"),"yyyy-MM-dd"))

  //emp_data_csv.show()
  //emp_data_csv.count()

  //emp_data_csv.printSchema()

  // Rename of existing dataframe column
  val empRename_data=emp_data_csv.withColumnRenamed("TIMESTAMP","Timestamp")
    .withColumnRenamed("RECORD","Record").withColumnRenamed("BP_kPa_Avg","Pressure")
    .withColumnRenamed("Precip_Tot","Rainfall").withColumnRenamed("AirTC_Avg","Temperature_Avg")
    .withColumnRenamed("AirTC","Temperature").withColumnRenamed("RH","RelativeHumidity")
    .withColumnRenamed("SoilT(1)","SoilTemp1").withColumnRenamed("SoilT(2)","SoilTemp2")
    .withColumnRenamed("SoilT(3)","SoilTemp3").withColumnRenamed("SoilT(4)","SoilTemp4")
    .withColumnRenamed("SoilMoist(1)","SoilMoist1").withColumnRenamed("SoilMoist(2)","SoilMoist2")
    .withColumnRenamed("SoilMoist(3)","SoilMoist3").withColumnRenamed("SoilMoist(4)","SoilMoist4")
    .withColumnRenamed("SoilMoist(5)","SoilMoist5").withColumnRenamed("SoilMoist(6)","SoilMoist6")
    .withColumnRenamed("Batt_Volt","BatteryVoltage")

  empRename_data.printSchema()

  empRename_data.select(empRename_data("Rainfall")).distinct().show()


  // Save is not working in windows
/*  emp_data_csv.coalesce(1).write.format("csv").option("header", "false")
               .mode(SaveMode.Overwrite).save("C:\\PSTL\\Purdue_PSTL\\Spark-Kafka Training\\temp")*/

  //emp_data_csv.write.csv("src\\main\\resources\\temp")

}
