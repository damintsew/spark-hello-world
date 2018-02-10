package df

class Common {

//  def toInt(s: String): Option[Int] = {
//    try {
//      Some(s.toInt)
//    } catch {
//      case e: Exception => None
//    }
//  }
//
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
}
