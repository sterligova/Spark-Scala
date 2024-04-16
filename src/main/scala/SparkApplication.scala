package spark

import org.apache.spark.sql.SparkSession

trait SparkApplication extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .getOrCreate()
}
