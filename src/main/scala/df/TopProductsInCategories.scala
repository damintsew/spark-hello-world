package df

// import required spark classes
import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.{SaveMode, SparkSession}
import java.util.Properties

import org.apache.spark.sql.functions._

// define main method (Spark entry point)
object TopProductsInCategories extends Common {

  val url = "jdbc:mysql://127.0.0.1:3306/adamintsev"

  val prop = new Properties()
  prop.put("user", "root")
  prop.put("password", "cloudera")

  case class Sale(id: Long, category: String, name: String, price: Double, date: String, ipaddr: String)


  def main(args: Array[String]) {

    // initialise spark context
    //    val spark = SparkSession
    //      .builder()
    //      .appName("Spark SQL basic example")
    //      .config("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    //      .getOrCreate()

    val conf = new SparkConf().setAppName("TopCategoriesDF")
    val sc = new SparkContext(conf)
    val spark = new org.apache.spark.sql.SQLContext(sc)

    import spark.implicits._
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    // do stuff
    println("************")
    println("Initializing application!")
    println("************")

    val saleDF = spark.sparkContext.textFile("/user/adamintsev/events/*")
      .map(_.split(";"))
      .map(v => (toLong(v(0)), v(1), v(2), toDouble(v(3)), v(4), v(5)))
      .filter(v => v._1.isDefined && v._4.isDefined)
      .map(v => Sale(v._1.get, v._2, v._3, v._4.get, v._5, v._6))
      .toDF()

    val w = Window.partitionBy("category").orderBy(desc("count"))
    val top10ProductsInCategories = saleDF
      .groupBy("category", "name") //.count()
      .agg(count("*").alias("count"))
      .withColumn("rn", row_number.over(w))
      .where("rn <= 10")
      .select("category", "name")

      top10ProductsInCategories.write.mode("append")
      .jdbc(url, "top_purchased_categories", prop)

    println("************")

  }
}

