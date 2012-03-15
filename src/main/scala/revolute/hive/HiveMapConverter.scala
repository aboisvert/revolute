package revolute.hive

import revolute.util.Converter
import scala.collection._

class HiveMapConverter[K, V](val collectionItemTerminator: String, val mapKeyTerminator: String)
  extends Converter[String, Map[String, String]]
{
  def apply(x: String): Map[String, String] = {
    val items = x.split(collectionItemTerminator)
    items map { item =>
      val parts = item.split(mapKeyTerminator)
      parts(0) -> parts(1)
    } toMap
  }
}
