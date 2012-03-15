package revolute.util

import scala.collection._
import scala.collection.JavaConversions

trait NamingContext {
  def nameFor(t: Any): String

  def overrideName(node: Any, newName: String): NamingContext = new NamingContext {
    def nameFor(n: Any) = if (n == node) newName else nameFor(n)
  }
}

object NamingContext {
  def apply() = new NamingContext {
    private val tnames = mutable.Map[Any, String]()
    private var nextTid = 1

    def nameFor(t: Any) = tnames.get(t) match {
      case Some(n) => n
      case None =>
        val n = "t" + nextTid
        nextTid += 1
        tnames.put(t, n)
        n
    }
  }
}