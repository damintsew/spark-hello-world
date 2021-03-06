package rdd


import common.Common.{ip2Long, toDouble, toLong}
import common.GeoInfoHandler.GeoInfoHandler
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.{SparkConf, SparkContext}

object TopPurchaseInCountriesV2 {

  val url = "jdbc:mysql://127.0.0.1:3306/adamintsev"
  val username = "root"
  val password = "cloudera"

  case class Struct(maxIp: Long, countryName: String)

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("TopPurchaseInCountriesV2")
    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    // do stuff
    println("************")
    println("Initializing application!")
    println("************")

    val salesRdd = sc.textFile("/user/adamintsev/events/*")
      .map(_.split(";"))
      .map(v => (toDouble(v(3)), ip2Long(v(5))))
      .filter(v => v._1 != -1 && v._2 != -1)

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

    val structBroadcast = sc.broadcast(geocodeStruct)

    val topCountries = salesRdd
      .map(sale => {
        val saleIp = sale._2
        val struct = structBroadcast.value

        val country = struct.getGeoId(saleIp)

        (country, sale._1)
      })
      .reduceByKey(_ + _)
      .takeOrdered(10)(Ordering[Double].reverse.on(x=>x._2))

    topCountries.foreach(println)

    println("************")

//    val conn = DriverManager.getConnection(url, username, password)
//
//    topCountries.foreach {
//      row =>
//        val statement = conn.prepareStatement("INSERT INTO top_sales_by_country (country, sales) VALUES (?,?)")
//
//        statement.setString(1, row._1)
//        statement.setDouble(2, row._2)
//        statement.executeUpdate
//
//        statement.close()
//    }
//
//    conn.close()

    sc.stop()
  }
}

