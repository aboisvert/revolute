package revolute.util

import scala.annotation.tailrec
import scala.collection._
import scala.collection.JavaConversions

sealed trait Identifier

object Identifier {
  case class Name(name: String)   extends Identifier
  case class Ref(id: Identifier)  extends Identifier
  class Anonymous(val prefix: String) extends Identifier
}

trait NamingContext {
  def nameFor(id: Identifier): String

  def overrideName(overrideId: Identifier, newName: String): NamingContext = new NamingContext {
    def nameFor(id: Identifier) = if (id == overrideId) newName else nameFor(id)
  }
}

object NamingContext {
  def apply() = new NamingContext {
    private val names = mutable.Map[Identifier, String]()
    private var nextTid = 1

    override def nameFor(t: Identifier) = names.getOrElse(t, t match {
      case a: Identifier.Anonymous =>
        val newName = a.prefix + "#" + nextTid
        nextTid += 1
        names.put(a, newName)
        newName
      case Identifier.Name(name) => name
      case Identifier.Ref(id)    => nameFor(id)
    })
  }
}

object StaticNamingContext extends NamingContext {
  @tailrec
  override def nameFor(t: Identifier) = t match {
    case a: Identifier.Anonymous => sys.error("Anonymous identifier reference within a static context")
    case Identifier.Name(name)   => name
    case Identifier.Ref(id)      => nameFor(id)
  }
}