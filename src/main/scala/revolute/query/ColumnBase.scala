package revolute.query

import revolute.QueryException
import revolute.util.{Node, WithOp}

/** Common base trait for columns, tables and projections (but not unions and joins) */
trait ColumnBase[T] extends Node with WithOp {
  def columnName: String = "col" + System.identityHashCode(this)
  override def nodeDelegate: Node = if (op eq null) this else op.nodeDelegate
}

/** Base class for columns */
abstract class Column[T: TypeMapper] extends ColumnBase[T] {
  final val typeMapper = implicitly[TypeMapper[T]]
  
  def orElse(n: =>T): Column[T] = new WrappedColumn[T](this) { }
  
  final def orFail = orElse { throw new QueryException("Read NULL value for column " + this) }
  
  def ? : Column[Option[T]] = new WrappedColumn(this)

  def getOr[U](n: => U)(implicit ev: Option[U] =:= T): Column[U] = error("todo") // new WrappedColumn[U](this)(new BaseTypeMapper[U] {}) { }
  
  def get[U](implicit ev: Option[U] =:= T): Column[U] = getOr[U] { throw new QueryException("Read NULL value for column "+this) }
  
  final def ~[U](b: Column[U]) = new Projection2[T, U](this, b)

  // Functions which don't need an OptionMapper
  //def in(e: Query[Column[_]]) = ColumnOps.In(Node(this), Node(e))
  //def notIn(e: Query[Column[_]]) = ColumnOps.Not(Node(ColumnOps.In(Node(this), Node(e))))
  def count = ColumnOps.Count(Node(this))
  def isNull = ColumnOps.Is(Node(this), ConstColumn.NULL)
  def isNotNull = ColumnOps.Not(Node(ColumnOps.Is(Node(this), ConstColumn.NULL)))
  def countDistinct = ColumnOps.CountDistinct(Node(this))
  def asColumnOf[U: TypeMapper]: Column[U] = ColumnOps.AsColumnOf[U](Node(this), None)
  def asColumnOfType[U: TypeMapper](typeName: String): Column[U] = ColumnOps.AsColumnOf[U](Node(this), Some(typeName))

  def asc = new Ordering.Asc(Node(this))
  def desc = new Ordering.Desc(Node(this))
}

object ConstColumn {
  def NULL =  new ConstColumn[AnyRef]("NULL", null)(TypeMapper.NullTypeMapper)
}

/** A column with a constant value (i.e., literal value) */
case class ConstColumn[T : TypeMapper](override val columnName: String, value: T) extends Column[T] {
  def nodeChildren = Nil
  override def toString = value match {
    case null => "ConstColumn null"
    case a: AnyRef => "ConstColumn["+a.getClass.getName+"] "+a
    case _ => "ConstColumn "+value
  }
}

/** A column which gets created as the result of applying an operator. */
abstract class OperatorColumn[T : TypeMapper] extends Column[T] {
  protected[this] val leftOperand: Node = Node(this)
}

/** A WrappedColumn can be used to change a column's nullValue. */
class WrappedColumn[T: TypeMapper](parent: ColumnBase[_]) extends Column[T] {
  override def nodeDelegate = if (op eq null) Node(parent) else op.nodeDelegate
  def nodeChildren = nodeDelegate :: Nil
}

/** A column which is part of a Table. */
class NamedColumn[T: TypeMapper](val table: TableBase[_], override val columnName: String, val options: ColumnOption[T]*) extends Column[T] {
  def nodeChildren = table :: Nil
  override def toString = "NamedColumn " + columnName
  override def nodeNamedChildren = (table, "table") :: Nil
}

object NamedColumn {
  def unapply[T](n: NamedColumn[T]) = Some((n.table, n.columnName, n.options))
}