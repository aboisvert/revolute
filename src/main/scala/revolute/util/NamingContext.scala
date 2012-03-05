package revolute.util

import cascading.flow.{FlowConnector, FlowProcess}
import revolute.query.Table
import revolute.util.Compat.Tap

import scala.collection._
import scala.collection.JavaConversions

trait NamingContext {
  def nameFor(t: Any): String

  def overrideName(node: Any, newName: String): NamingContext = new NamingContext {
    def nameFor(n: Any) = if (n == node) newName else nameFor(n)
  }

  val tableBindings = mutable.Map[Table[_], Tap]()

  def sources: java.util.Map[String, Tap] = {
    val taps: Map[String, Tap] = tableBindings map { case (table, tap) => (table.tableName, tap: Tap) } toMap;
    JavaConversions.mapAsJavaMap(taps)
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