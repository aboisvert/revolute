package revolute.query

import revolute.util.{Identifier, Tuple}
import scala.collection._
import scala.collection.mutable.ArrayBuffer

trait EvaluationContext {
  def nextTuple(): Tuple
  def position(c: ColumnBase[_]): Int
}

object NullFilter extends (Any => Any) {
  override def apply(value: Any) = value
}

object OptionFilter extends (Any => Any) {
  override def apply(value: Any) = value match {
    case Some(x) => x
    case None    => null
    case null    => null
  }
}

class DefaultValueFilter(val defaultValue: Any) extends (Any => Any) {
  override def apply(value: Any) = if (value != null) value else defaultValue
}

class EvaluationFilter(
  val parent: EvaluationContext,
  val filterPositions: Array[Int],
  val filters: Array[Any => Any]
) extends EvaluationContext {
  private[this] val values = new Array[Any](filterPositions.length)

  private[this] var tuple: Tuple = null

  private[this] val overrideTuple = new Tuple {
    override def get(pos: Int): Any = {
      var i = 0
      while (i < filterPositions.length) {
        if (pos == filterPositions(i)) return values(i)
        i += 1
      }
      tuple.get(pos)
    }
  }

  override def nextTuple(): Tuple = {
    tuple = parent.nextTuple()
    if (tuple == null) return null

    var i = 0
    while (i < filterPositions.length) {
      val pos = filterPositions(i)
      val filter = filters(i)

      val value = tuple.get(pos)
      val filtered = filter(value)

      if (filtered == null) return null

      values(i) = filtered
      i += 1
    }

    overrideTuple
  }

  override def position(c: ColumnBase[_]) = parent.position(c)
}
