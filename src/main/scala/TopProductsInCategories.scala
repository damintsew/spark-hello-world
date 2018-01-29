
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet}
import org.apache.spark.rdd.PairRDDFunctions


// define main method (Spark entry point)
object TopProductsInCategories {

  val url = "jdbc:mysql://127.0.0.1:3306/adamintsev"
  val username = "root"
  val password = "cloudera"


  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("TopProductsInCategories")
    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    // do stuff
    println("************")
    println("Initializing application!")
    println("************")

    val topCategoriesWithCount = sc.textFile("/user/adamintsev/events/*")
      .map(_.split(";"))
      .map(v => ((v(1), v(2)), 1))
      .aggregateByKey(0)((accum, v) => accum + v, _ + _)
      .sortBy(v => (v._1._1,v._2), ascending = false)
      .groupBy(_._1._1)
      .map(v => (v._1, v._2.slice(0, 10)))
      .flatMap(_._2)
      .map(v=> (v._1._1, v._1._2, v._2))

    println("************")

    topCategoriesWithCount.foreachPartition(partition => {
      val conn = DriverManager.getConnection(url, username, password)

      partition.foreach {
        row =>
          val statement = conn.prepareStatement("INSERT INTO top_products_for_categories (category, product_name) VALUES (?,?)")

          statement.setString(1, row._1)
          statement.setString(2, row._2)
          statement.executeUpdate

          statement.close()
      }

      conn.close()
    })



    // terminate

    sc.stop()

  }
}

