import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class OlistBrazilian(
  product_category_name: String,
  seller_id: String,
  seller_city: String,
  seller_state: String,
  review_score: String,
  review_comment_message: String,
  review_creation_date: String,
  order_delivered_customer_date: java.sql.Timestamp
)


object olistbr extends App {

 val spark = SparkSession
.builder()
.appName("App Name")
.master("local")
.getOrCreate()   

def readCSV(path: String): DataFrame = {
    spark.read
      .format("csv")
      .option("InFerschema", "true")
      .option("header", "true")
      .csv(path)
  }

// Read all CSV files
  val reviewsWithNull: DataFrame = readCSV("C:/Users/isterligova/test_spark_scala/src/archive/olist_order_reviews_dataset.csv")
  val products: DataFrame = readCSV("C:/Users/isterligova/test_spark_scala/src/archive/olist_products_dataset.csv")
  val sellers: DataFrame = readCSV("C:/Users/isterligova/test_spark_scala/src/archive/olist_sellers_dataset.csv")
  val orderDelivered: DataFrame = readCSV("C:/Users/isterligova/test_spark_scala/src/archive/olist_orders_dataset.csv")
  val orderItems: DataFrame = readCSV("C:/Users/isterligova/test_spark_scala/src/archive/olist_order_items_dataset.csv")


def removeNullReviewScoreRows(df: DataFrame): DataFrame = {
  df.na.drop(Seq("review_score"))
}

//Threshold for considering a review as "bad". 
//Any review with a score equal to or below this threshold will be considered bad.
val badReviewThreshold = 1 

//Threshold for considering a review as "bad". 
//Any review with a score equal to or below this threshold will be considered bad.
def filterBadReviews(df: DataFrame, threshold: Int): DataFrame = {
  df.filter(col("review_score") <= threshold)
}


def mergeDataFrames(orderItems: DataFrame, products: DataFrame, sellers: DataFrame, orderDelivered: DataFrame): DataFrame => DataFrame = {
  (reviews: DataFrame) =>
    reviews
      .join(orderItems, reviews("order_id") === orderItems("order_id"), "left")
      .join(products, orderItems("product_id") === products("product_id"), "left")
      .join(sellers, orderItems("seller_id") === sellers("seller_id"), "left")
      .join(orderDelivered, reviews("order_id") === orderDelivered("order_id"), "left")
      .select(
        products("product_category_name"),
        sellers("seller_id"),
        sellers("seller_city"),
        sellers("seller_state"),
        reviews("review_score"),
        reviews("review_comment_message"),
        reviews("review_creation_date"),
        orderDelivered("order_delivered_customer_date")
      )
}



val reviewsResultDF = reviewsWithNull
.transform(removeNullReviewScoreRows)
.transform(filterBadReviews(_, badReviewThreshold))
.transform(mergeDataFrames(orderItems, products, sellers, orderDelivered))



//Optional, creating a Dataset of type OlistBrazilian from the DataFrame
//import spark.implicits._
//  val badReviewsDS = reviewsResultDF.as[OlistBrazilian]
//badReviewsDS.show()

}
