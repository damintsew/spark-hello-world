package common

import java.util

import common.Common.ip2Long

object GeoInfoHandler extends Serializable {

  private class Pair (val end: Long, val geoId: String) extends Serializable {

  }

  class GeoInfoHandler extends Serializable {
    final private val map: util.TreeMap[Long, Pair] = new util.TreeMap[Long, Pair]

    def addNetwork(startIp: Long, endIp: Long, geoId: String): Unit = {
      map.put(startIp, new Pair(endIp, geoId))
    }

    def getGeoId(ip: String): String = {
      val parsed: Long = ip2Long(ip)
      getGeoId(parsed)
    }

    def getGeoId(ip: Long): String = {
      val entry: util.Map.Entry[Long, Pair] = map.floorEntry(ip)
      if (entry != null && ip <= entry.getValue.end) new String(entry.getValue.geoId)
      else null
    }
  }

}