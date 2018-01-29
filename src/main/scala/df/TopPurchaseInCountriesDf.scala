package df


import java.sql.DriverManager
import java.util.Properties

import df.TopProductsInCategories.{toDouble, toLong}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._




// define main method (Spark entry point)
object TopPurchaseInCountriesDf {

  val url = "jdbc:mysql://127.0.0.1:3306/adamintsev"

  val prop = new java.util.Properties()
  prop.put("user", "root")
  prop.put("password", "cloudera")

  case class Sale(id: Long, category: String, name: String, price: Double, date: String, ipaddr: String)
  case class Geocode(ipaddr: String, geocodeId: String)
  case class Country(geocodeId: String, countryName: String)

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("TopPurchaseInCountriesDf")
    val sc = new SparkContext(conf)
    val spark = new org.apache.spark.sql.SQLContext(sc)

    import spark.implicits._
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    // do stuff
    println("************")
    println("Initializing application!")
    println("************")

    val saleDF = sc.textFile("/user/adamintsev/events/*")
      .map(_.split(";"))
      .map(v => (toLong(v(0)), v(1), v(2), toDouble(v(3)), v(4), v(5)))
      .filter(v => v._1.isDefined && v._4.isDefined)
      .map(v => Sale(v._1.get, v._2, v._3, v._4.get, v._5, v._6))
      .toDF()

    val saleGroupedByIpDF = saleDF.groupBy("ipaddr")
      .agg(sum("price").alias("price"))

    val ip_geocodeDF = sc.textFile("/user/adamintsev/geo/ip")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(_.split(","))
      .map(v => Geocode(v(0), v(1)))
      .toDF()

    val joinedWithGeocode = saleGroupedByIpDF
      .join(ip_geocodeDF, "ipaddr")
      .groupBy("geocodeId")
      .agg(sum("price").alias("price"))
      .select("geocodeId", "price")


    val countriesDF = sc.textFile("/user/adamintsev/geo/country")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(_.split(","))
      .map(v => Country(v(0), v(5)))
      .toDF()

    val topCountries = joinedWithGeocode
      .join(countriesDF, "geocodeId")
      .groupBy(column("countryName").as("country"))
      .agg(sum("price").alias("sales"))
      .orderBy(desc("sales"))


    println("************")

    topCountries.write.mode("append")
      .jdbc(url, "top_sales_by_country", prop)

    // terminate
    sc.stop()
  }
}

