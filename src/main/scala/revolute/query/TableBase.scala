package revolute.query

import revolute.QueryException

sealed trait TableBase[T]

abstract class AbstractTable[T](val tableName: String) extends TableBase[T] with ColumnBase[T] {
  def * : ColumnBase[T]

  override def toString = "Table " + tableName

  final override def tables = Set(this)

  final override def equals(other: Any) = other match {
    case t: AbstractTable[_] => t.tableName == this.tableName
  }

  final override def hashCode = tableName.hashCode
}

object AbstractTable {
  def unapply[T](t: AbstractTable[T]) = Some(t.tableName)
}

final class JoinBase[+T1 <: AbstractTable[_], +T2 <: TableBase[_]](_left: T1, _right: T2, joinType: Join.JoinType) {
  def nodeChildren = _left :: _right :: Nil
  override def toString = "JoinBase(" + _left + "," + _right + ")"
  def on[T <: Column[_] : CanBeQueryCondition](pred: (T1, T2) => T) = new Join(_left, _right, joinType, pred(_left, _right))
}

final class Join[+T1 <: AbstractTable[_], +T2 <: TableBase[_]](
  val left: T1,
  val right: T2,
  val joinType: Join.JoinType,
  val on: Any
) extends TableBase[Nothing] {
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
