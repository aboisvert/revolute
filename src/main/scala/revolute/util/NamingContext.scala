package revolute.util

import cascading.tap.Tap
import revolute.query.Table
import scala.collection._

trait NamingContext {
  def nameFor(t: Any): String

  def overrideName(node: Any, newName: String): NamingContext = new NamingContext {
    def nameFor(n: Any) = if (n == node) newName else nameFor(n)
  }

  val tableBindings = mutable.Map[Table[_], Tap]()

  def sources = {
    scala.collection.JavaConversions.asJavaMap(tableBindings map { case (table, tap) => (table.tableName, tap) })
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