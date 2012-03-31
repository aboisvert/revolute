package revolute.query

import cascading.tuple.TupleEntry
import scala.collection._
import revolute.util.Identifier

sealed trait TableBase[T <: Product] extends ColumnBase[T] with java.io.Serializable {
  // def operationType = OperationType.PureMapper
}

abstract class AbstractTable[T <: Product](val tableName: String) extends TableBase[T] {
  def * : Projection[T]

  override def nameHint = tableName
  override def dependencies = *.dependencies
  override val operationType = OperationType.PureMapper
  override def chainEvaluation(context: EvaluationContext) = *.chainEvaluation(context)

  final override def equals(other: Any) = other match {
    case t: AbstractTable[_] => t.tableName == this.tableName
    case _ => false
  }

  final override def hashCode = tableName.hashCode

  override def toString = "Table " + tableName
}

final class JoinBase[+T1 <: AbstractTable[_], +T2 <: TableBase[_]](_left: T1, _right: T2, joinType: Join.JoinType) {
  override def toString = "JoinBase(" + _left + "," + _right + ")"
  def on[T <: ColumnBase[_]](pred: (T1, T2) => T) = new Join(_left, _right, joinType, pred(_left, _right))
}


final class Join[+T1 <: AbstractTable[_], +T2 <: TableBase[_]](
  val left: T1,
  val right: T2,
  val joinType: Join.JoinType,
  val on: ColumnBase[_]
) extends TableBase[Nothing] {
  override val nameHint = "Join"
  override def dependencies = Set(left, right)
  override val operationType = OperationType.PureMapper
  override def chainEvaluation(context: EvaluationContext) = sys.error("shouldn't be called")
  override def toString = "Join(%s, %s)" format (left, right)
}

object Join {
  def unapply[T1 <: AbstractTable[_], T2 <: TableBase[_]](j: Join[T1, T2]) = Some((j.left, j.right))

  abstract class JoinType(val joinType: String)
  case object Inner extends JoinType("inner")
  case object Left  extends JoinType("left outer")
  case object Right extends JoinType("right outer")
  case object Outer extends JoinType("full outer")
}
