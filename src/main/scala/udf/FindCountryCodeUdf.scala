package udf

import java.net.URL
import java.util
import java.util.List

import com.google.common.base.{Charsets, Throwables}
import com.google.common.io.Resources
import common.Common
import org.apache.commons.net.util.SubnetUtils
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.hive.ql.udf.UDFType
import udf.GeoInfoHandler.GeoInfoHandler


@UDFType(deterministic = false, stateful = true)
class FindCountryCodeUdf extends UDF {

  private val SOURCE_FILE = "ipv4_dataset.csv"
  private var geoInfoHandler: GeoInfoHandler = null

  def evaluate(input: String): String = {
    try {
      System.out.println("GeoInfoHandler initialized")
      if (geoInfoHandler == null) geoInfoHandler = init()

      return geoInfoHandler.getGeoId(input.trim)
    } catch {
      case e: Throwable =>
        System.err.println(e.getMessage)
    }
    null
  }

  private def init(): GeoInfoHandler =  {
    val map = new GeoInfoHandler
    try {
      val url = Resources.getResource(SOURCE_FILE)
      System.out.println(url)
      var lines = Resources.readLines(url, Charsets.UTF_8)
      //skip header
      lines = lines.subList(1, lines.size)
      import scala.collection.JavaConversions._

      for (line <- lines) {
        val parts = line.split(",")
        if (parts.length >= 6) {
          val utils = new SubnetUtils(parts(0).trim)
          val begin = Common.ip2Long(utils.getInfo.getLowAddress)
          val end = Common.ip2Long(utils.getInfo.getHighAddress)

          map.addNetwork(begin, end, parts(1).trim)
        }
      }
    } catch {
      case e: Exception =>
        System.out.println(e.getMessage)
        Throwables.propagate(e)
    }
    map
  }
}

