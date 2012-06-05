package revolute.util

import scala.collection._

class DistinctMap[T](val distinctIds: mutable.Map[T, mutable.Set[String]] = mutable.Map[T, mutable.Set[String]]()) {
  def distinctId(x: T, baseName: String): String = {
    val names = distinctIds.getOrElseUpdate(x, mutable.Set())
    if (names.isEmpty) {
      names += baseName
      baseName
    } else {
      var i = 2
      var name = baseName + "#" + i
      while (names contains name) {
        i += 1
        name = baseName + "#" + i
      }
      names += name
      name
    }
  }
}
