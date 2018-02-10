
import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}


// define main method (Spark entry point)
object TopPurchaseInCountries {

  val url = "jdbc:mysql://127.0.0.1:3306/adamintsev"
  val username = "root"
  val password = "cloudera"

//  def toDouble(s: String): Option[Double] = {
//    try {
//      Some(s.toDouble)
//    } catch {
//      case e: Exception => None
//    }
//  }

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("TopPurchaseInCountries")
    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    // do stuff
    println("************")
    println("Initializing application!")
    println("************")

    val topCategoriesWithCount = sc.textFile("/user/adamintsev/events/*")
      .map(_.split(";"))
//      .map(v => (v(5), toDouble(v(3))))
      .map(v => (v(5), v(3).toDouble))
//      .filter(v => v._2.isDefined)
//      .map(v => (v._1, v._2.get))
      .aggregateByKey(0.0)((accum, v) => accum + v, _ + _)

    val ip_geocode = sc.textFile("/user/adamintsev/geo/ip")
      .map(_.split(","))
      .map(v => (v(0), v(1)))

    var joinedWithGeocode = topCategoriesWithCount
      .join(ip_geocode)
      .map(v => (v._2._2, v._2._1))
      .aggregateByKey(0.0)(_ + _, _ + _)

    val countries = sc.textFile("/user/adamintsev/geo/country")
      .map(_.split(","))
      .map(v => (v(0), v(5)))

    var topCountries = joinedWithGeocode
      .join(countries)
      .map(v => (v._2._2, v._2._1))
      .reduceByKey(_ + _)
      .takeOrdered(10)(Ordering[Double].reverse.on(x => x._2))

    println("************")

    val conn = DriverManager.getConnection(url, username, password)

    topCountries.foreach {
      row =>
        val statement = conn.prepareStatement("INSERT INTO top_sales_by_country (country, sales) VALUES (?,?)")

        statement.setString(1, row._1)
        statement.setDouble(2, row._2)
        statement.executeUpdate

        statement.close()
    }

    conn.close()

    // terminate
    sc.stop()
  }
}

