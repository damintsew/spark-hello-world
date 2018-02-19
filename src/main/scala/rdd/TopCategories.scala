package rdd

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}

object TopCategories {

  val url = "jdbc:mysql://127.0.0.1:3306/adamintsev"
  val username = "root"
  val password = "cloudera"

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("TopCategories")
    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    // do stuff
    println("************")
    println("Initializing application!")
    println("************")

    val topCategories = sc.textFile("/user/adamintsev/events/*")
      .map(_.split(";"))
      .map(v => (v(1), 1))
      .aggregateByKey(0)((accum, v) => accum + v, _ + _)
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .collect()
      .slice(0, 10)

    topCategories.foreach(println)
    println("************")

    val conn = DriverManager.getConnection(url, username, password)

    topCategories.foreach {
      category =>
        val statement = conn.prepareStatement("INSERT INTO top_purchased_categories (category, quantity) VALUES (?,?) ")

        statement.setString(1, category._1)
        statement.setLong(2, category._2)
        statement.executeUpdate

        statement.close()
    }

    // terminate
    conn.close()
    sc.stop()

  }
}

