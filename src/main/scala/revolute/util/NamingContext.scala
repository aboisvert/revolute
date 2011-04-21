package revolute.util

import cascading.tap.Tap
import revolute.query.Table
import scala.collection._

trait NamingContext {
  def nameFor(t: Node): String

  def overrideName(node: Node, newName: String): NamingContext = new NamingContext {
    def nameFor(n: Node) = if (n == node) newName else nameFor(n)
  }
  
  val tableBindings = mutable.Map[Table[_], Tap]()
  
  def sources = {
    scala.collection.JavaConversions.asJavaMap(tableBindings map { case (table, tap) => (table.tableName, tap) })
  }
}

object NamingContext {
  def apply() = new NamingContext {
    private val tnames = mutable.Map[RefId[Node], String]()
    private var nextTid = 1

    def nameFor(t: Node) = tnames.get(new RefId(t)) match {
      case Some(n) => n
      case None =>
        val n = "t" + nextTid
        nextTid += 1
        tnames.put(new RefId(t), n)
        n
    }
  }
}