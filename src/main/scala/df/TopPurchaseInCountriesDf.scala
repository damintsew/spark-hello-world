//package df
//
//
//import java.sql.DriverManager
//import java.util.Properties
//
//import df.TopProductsInCategories.{ip2Long, toDouble, toLong}
//import org.apache.commons.net.util.SubnetUtils
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.functions._
//import org.apache.commons.net.util.SubnetUtils
//
//import scala.collection.immutable.TreeMap
//
//
//// define main method (Spark entry point)
//object TopPurchaseInCountriesDf {
//
//  val url = "jdbc:mysql://127.0.0.1:3306/adamintsev"
//
//  val prop = new java.util.Properties()
//  prop.put("user", "root")
//  prop.put("password", "cloudera")
//
//  case class Sale(id: Long, category: String, name: String, price: Double, date: String, ipaddr: Long)
//  case class Geocode(ipaddr: String, geocodeId: String)
//  case class Country(geocodeId: String, countryName: String)
//
//  def main(args: Array[String]) {
//
//    // initialise spark context
//    val conf = new SparkConf().setAppName("TopPurchaseInCountriesDf")
//    val sc = new SparkContext(conf)
//    val spark = new org.apache.spark.sql.SQLContext(sc)
//
//    import spark.implicits._
//    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
//
//    // do stuff
//    println("************")
//    println("Initializing application!")
//    println("************")
//
//    val saleDF = sc.textFile("/user/adamintsev/events/*")
//      .map(_.split(";"))
//      .map(v => (toLong(v(0)), v(1), v(2), toDouble(v(3)), v(4), ip2Long(v(5))))
//      .filter(v => v._1 != -1 && v._4 != -1)
//      .map(v => Sale(v._1, v._2, v._3, v._4, v._5, v._6))
//      .toDF()
//
//    val saleGroupedByIpDF = saleDF.groupBy("ipaddr")
//      .agg(sum("price").alias("price"))
//
//    val geocodeList = sc.textFile("/user/adamintsev/geo/ip")
//      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
//      .map(_.split(","))
//      .map(v => {
//        val subnet = new SubnetUtils(v(0))
//        val high = ip2Long(subnet.getInfo.getHighAddress)
//        val low = ip2Long(subnet.getInfo.getLowAddress)
//
//        (v(1).toLong, low, high)
//      })
//      .toDF("geocodeId", "minIp", "maxIp")
//
//    val countriesDF = sc.textFile("/user/adamintsev/geo/country")
//      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
//      .map(_.split(","))
//      .map(v => Country(v(0), v(5)))
//      .toDF()
//
//    val geocodeWithCountry = geocodeList.join(countriesDF, "geocodeId")
//      .collect()
//
////    var tree = new java.util.TreeMap[Long, Struct]
////
////    geocodeWithCountry.foreach(code => {
////      tree.put(code(0), Struct(code(1), code(2)))
////    })
//
//
////    val tree = new TreeMap[Long, Long]()
////    geocodeList.foreach(geo => {
////      geo.
////    })
//
//
////    val joinedWithGeocode = saleGroupedByIpDF
////      .join(ip_geocodeDF, "ipaddr")
////      .groupBy("geocodeId")
////      .agg(sum("price").alias("price"))
////      .select("geocodeId", "price")
//
//
////    val countriesDF = sc.textFile("/user/adamintsev/geo/country")
////      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
////      .map(_.split(","))
////      .map(v => Country(v(0), v(5)))
////      .toDF()
//
////    val topCountries = joinedWithGeocode
////      .join(countriesDF, "geocodeId")
////      .groupBy(column("countryName").as("country"))
////      .agg(sum("price").alias("sales"))
////      .orderBy(desc("sales"))
//
//
//    println("************")
////    topCountries.show()
////
////    topCountries.write.mode("append")
////      .jdbc(url, "top_sales_by_country", prop)
//
//    // terminate
//    sc.stop()
//  }
//}
//