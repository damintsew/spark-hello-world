package df

class Common {

  def toLong(s: String): Long = {
    try {
      s.toLong
    } catch {
      case e: Exception => -1
    }
  }

  def toDouble(s: String): Double = {
    try {
      s.toDouble
    } catch {
      case e: Exception => -1
    }
  }

  def ip2Long(ipAddress: String): Long = {
    val ipAddressInArray = ipAddress.split("\\.")
    var result = 0L

    try {
      for (i <- 0 to ipAddressInArray.length - 1) {
        val power = 3 - i
        val ip = ipAddressInArray(i).toInt
        val longIP = (ip * Math.pow(256, power)).toLong
        result = result + longIP
      }
    } catch {
      case e:Exception => return -1

    }
    result
  }
}
