package udf

import java.util
import java.util.TreeMap

import udf.Common.ip2Long

object GeoInfoHandler {

  private class Pair (val end: Long, val geoId: String) {

  }

  class GeoInfoHandler {
    final private val map: util.TreeMap[Long, Pair] = new util.TreeMap[Long, Pair]

    def addNetwork(startIp: Long, endIp: Long, geoId: String): Unit = {
      map.put(startIp, new Pair(endIp, geoId))
    }

    def getGeoId(ip: String): String = {
      val parsed: Long = ip2Long(ip)
      val entry: util.Map.Entry[Long, Pair] = map.floorEntry(parsed)
      if (entry != null && parsed <= entry.getValue.end) new String(entry.getValue.geoId)
      else null
    }
  }

}