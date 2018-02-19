package df


import java.sql.DriverManager
import java.util.Properties

import df.TopProductsInCategories.{ip2Long, toDouble, toLong}
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.commons.net.util.SubnetUtils
import _root_.udf.GeoInfoHandler.GeoInfoHandler

import scala.collection.immutable.TreeMap


// define main method (Spark entry point)
object TopPurchaseInCountriesDf {

  val url = "jdbc:mysql://127.0.0.1:3306/adamintsev"

  val prop = new java.util.Properties()
  prop.put("user", "root")
  prop.put("password", "cloudera")

  case class Sale(id: Long, category: String, name: String, price: Double, date: String, ipaddr: Long)
  case class Geocode(ipaddr: String, geocodeId: String)
  case class Country(geocodeId: String, countryName: String)

  import org.apache.spark.sql.api.java.UDF1



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

    val ip_geocodes = sc.textFile("/user/adamintsev/geo/ip")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(_.split(","))
      .map(v => {
        val subnet = new SubnetUtils(v(0))
        val high = ip2Long(subnet.getInfo.getHighAddress)
        val low = ip2Long(subnet.getInfo.getLowAddress)

        (toLong(v(1)), (low, high))
      })
      .filter(v => v._1 != -1)

    val countries = sc.textFile("/user/adamintsev/geo/country")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(_.split(","))
      .map(v => (toLong(v(0)), v(5)))

    val countriesWithIpArray = ip_geocodes.join(countries)
      .map(_._2).collect()

    val geocodeStruct = new GeoInfoHandler()

    countriesWithIpArray.foreach(v => {
      //low, high, countryCode
      geocodeStruct.addNetwork(v._1._1, v._1._2, v._2)
    })

    import org.apache.spark.sql.api.java.UDF1

    val extractGeonameUdf = new UDF1[Long, String]() {
      @throws[Exception]
      override def call(ipAddrLong: Long): String = geocodeStruct.getGeoId(ipAddrLong)
    }

    import org.apache.spark.sql.types.DataTypes
    spark.udf.register("extractGeoname", extractGeonameUdf, DataTypes.StringType)

    val saleDF = sc.textFile("/user/adamintsev/events/*")
      .map(_.split(";"))
      .map(v => (toLong(v(0)), v(1), v(2), toDouble(v(3)), v(4), ip2Long(v(5))))
      .filter(v => v._1 != -1 && v._4 != -1)
      .map(v => Sale(v._1, v._2, v._3, v._4, v._5, v._6))
      .toDF()

    val topCountries = saleDF
      .withColumn("countryName", callUDF("extractGeoname", column("ipaddr")))
      .groupBy("countryName")
      .agg(sum("price").alias("price"))
      .orderBy(desc("price"))


    topCountries.show()


    println("************")

//    topCountries.write.mode("append")
//      .jdbc(url, "top_sales_by_country", prop)

    // terminate
    sc.stop()
  }
}

