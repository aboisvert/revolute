package revolute.query

import revolute.QueryException
import revolute.util.{Node, UnaryNode, BinaryNode, WithOp}

sealed trait TableBase[T] extends Node with WithOp {
  override def isNamedTable = true
}

abstract class AbstractTable[T](val tableName: String) extends TableBase[T] with ColumnBase[T] {
  def nodeChildren = Nil

  def * : ColumnBase[T]

  override def toString = "Table " + tableName
}

object AbstractTable {
  def unapply[T](t: AbstractTable[T]) = Some(t.tableName)
}

final class JoinBase[+T1 <: AbstractTable[_], +T2 <: TableBase[_]](_left: T1, _right: T2, joinType: Join.JoinType) {
  def nodeChildren = Node(_left) :: Node(_right) :: Nil
  override def toString = "JoinBase(" + Node(_left) + "," + Node(_right) + ")"
  def on[T <: Column[_] : CanBeQueryCondition](pred: (T1, T2) => T) = new Join(_left, _right, joinType, Node(pred(_left, _right)))
}

final class Join[+T1 <: AbstractTable[_], +T2 <: TableBase[_]](_left: T1, _right: T2,
  val joinType: Join.JoinType, val on: Node) extends TableBase[Nothing] {

  def left = _left.mapOp(n => Join.JoinPart(Node(n), Node(this)))
  def right = _right.mapOp(n => Join.JoinPart(Node(n), Node(this)))
  def leftNode = Node(_left)
  def rightNode = Node(_right)
  def nodeChildren = leftNode :: rightNode :: Nil
  override def toString = "Join(" + Node(_left) + "," + Node(_right) + ")"
}

object Join {
  def unapply[T1 <: AbstractTable[_], T2 <: TableBase[_]](j: Join[T1, T2]) = Some((j.left, j.right))

  final case class JoinPart(left: Node, right: Node) extends BinaryNode {
    override def toString = "JoinPart"
    override def nodeNamedChildren = (left, "table") :: (right, "from") :: Nil
  }

  abstract class JoinType(val joinType: String)
  case object Inner extends JoinType("inner")
  case object Left  extends JoinType("left outer")
  case object Right extends JoinType("right outer")
  case object Outer extends JoinType("full outer")
}
