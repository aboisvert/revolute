package revolute.query

import cascading.tuple.TupleEntry
import scala.collection._
import revolute.util.Identifier

sealed trait TableBase[T <: Product] extends Projection[T] with java.io.Serializable {
}

abstract class AbstractTable[T <: Product](val tableName: String) extends TableBase[T] {
  def * : Projection[T]

  override val nameHint = tableName
  override def dependencies: Set[ColumnBase[_]] = *.dependencies
  override val operationType = OperationType.PureMapper
  override def chainEvaluation(context: EvaluationContext) = *.chainEvaluation(context)

  override def columns = *.columns
  override def projectionArity = columns.size

  final override def equals(other: Any) = other match {
    case t: AbstractTable[_] => t.tableName == this.tableName
    case _ => false
  }

  final override def hashCode = tableName.hashCode

  override def toString = "Table " + tableName
}

final class JoinBase[+T1 <: Query[_], +T2 <: Query[_]](_left: T1, _right: T2, joinType: Join.JoinType) {
  override def toString = "JoinBase(" + _left + "," + _right + ")"
  def on[T <: ColumnBase[_]](pred: (T1, T2) => T) = new Join(_left, _right, joinType, pred(_left, _right))
}


final class Join[+T1 <: Query[_], +T2 <: Query[_]](
  val left: T1,
  val right: T2,
  val joinType: Join.JoinType,
  val on: ColumnBase[_]
) extends TableBase[Nothing] with QueryModifier {
  override val nameHint = "Join"
  override def dependencies = Set(left, right)
  override val operationType = OperationType.PureMapper
  override def chainEvaluation(context: EvaluationContext) = sys.error("shouldn't be called")
  override def columns = columns(left) ++ columns(right)
  override def projectionArity = columns.size
  override def toString = "Join(%s, %s)" format (left, right)

  private def columns(c: ColumnBase[_]): Seq[ColumnBase[_]] = c match {
    case p: Projection[_] => p.columns
    case other            => Seq(other)
  }
}

object Join {
  def unapply[T1 <: Query[_], T2 <: Query[_]](j: Join[T1, T2]): Option[(T1, T2)] = Some((j.left, j.right))

  abstract class JoinType(val joinType: String)
  case object Inner extends JoinType("inner")
  case object Left  extends JoinType("left outer")
  case object Right extends JoinType("right outer")
  case object Outer extends JoinType("full outer")
}
