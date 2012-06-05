package revolute.util

import scala.collection._

class DistinctNames(val distinctNames: mutable.Set[String] = mutable.Set()) {
  def distinctName(baseName: String): String = {
    if (!distinctNames.contains(baseName)) {
      distinctNames += baseName
      baseName
    } else {
      var i = 2
      var name = baseName + "#" + i
      while (distinctNames contains name) {
        i += 1
        name = baseName + "#" + i
      }
      distinctNames += name
      name
    }
  }
}