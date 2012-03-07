package revolute.query

import scala.collection._

sealed trait TableBase[T <: Product] extends ColumnBase[T] with NotAnExpression with java.io.Serializable

abstract class AbstractTable[T <: Product](val tableName: String) extends TableBase[T] {
  def * : Projection[T]

  override def toString = "Table " + tableName

  final override def tables = Set(this)

  final override def equals(other: Any) = other match {
    case t: AbstractTable[_] => t.tableName == this.tableName
  }

  final override def hashCode = tableName.hashCode

  implicit def columnToProjection[T](c: Column[T]): Projection[Tuple1[T]] = new Projection1[T](c)

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
) extends TableBase[Nothing] with NotAnExpression {
  override def tables = left.tables ++ right.tables
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
