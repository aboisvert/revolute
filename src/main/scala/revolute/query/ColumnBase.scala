package revolute.query

import revolute.QueryException

/** Common base trait for columns, tables and projections (but not unions and joins) */
trait ColumnBase[+T] {
  def columnName: Option[String] = None
}

/** Base class for columns */
abstract class Column[T: TypeMapper] extends ColumnBase[T] {
  final val typeMapper = implicitly[TypeMapper[T]]
  
  def orElse(n: =>T): Column[T] = new WrappedColumn[T](this) { }
  
  final def orFail = orElse { throw new QueryException("Read NULL value for column " + this) }
  
  def ? : Column[Option[T]] = error("todo") // new WrappedColumn[](this)(createOptionTypeMapper)

  def getOr[U](n: => U)(implicit ev: Option[U] =:= T): Column[U] = error("todo") // new WrappedColumn[U](this)(new BaseTypeMapper[U] {}) { }
  
  def get[U](implicit ev: Option[U] =:= T): Column[U] = getOr[U] { throw new QueryException("Read NULL value for column "+this) }
  
  final def ~[U](b: Column[U]) = new Projection2[T, U](this, b)

  // Functions which don't need an OptionMapper
  //def in(e: Query[Column[_]]) = ColumnOps.In(this, e)
  //def notIn(e: Query[Column[_]]) = ColumnOps.Not(ColumnOps.In(this, e))
  def count = ColumnOps.Count(this)
  def isNull = ColumnOps.Is(this, ConstColumn.NULL)
  def isNotNull = ColumnOps.Not(ColumnOps.Is(this, ConstColumn.NULL))
  def countDistinct = ColumnOps.CountDistinct(this)
  def asColumnOf[U: TypeMapper]: Column[U] = ColumnOps.AsColumnOf[U](this, None)
  def asColumnOfType[U: TypeMapper](typeName: String): Column[U] = ColumnOps.AsColumnOf[U](this, Some(typeName))

  def asc = new Ordering.Asc(By(this))
  def desc = new Ordering.Desc(By(this))
}

object ConstColumn {
  def NULL = apply[AnyRef]("NULL", null)(TypeMapper.NullTypeMapper)
  
  def apply[T : TypeMapper](columnName: String, value: T): ConstColumn[T] = ConstColumn(Some(columnName), value)
  def apply[T : TypeMapper](value: T): ConstColumn[T] = ConstColumn(None, value)
}

/** A column with a constant value (i.e., literal value) */
case class ConstColumn[T : TypeMapper](override val columnName: Option[String], value: T) extends Column[T] {
  override def toString = value match {
    case null => "ConstColumn null"
    case a: AnyRef => "ConstColumn["+a.getClass.getName+"] "+a
    case _ => "ConstColumn "+value
  }
}

/** A column which gets created as the result of applying an operator. */
abstract class OperatorColumn[T : TypeMapper] extends Column[T] {
  val leftOperand: ColumnBase[_] = this
}

/** A WrappedColumn can be used to change a column's nullValue. */
class WrappedColumn[T: TypeMapper](parent: ColumnBase[_]) extends Column[T] {
}

/** A column which is part of a Table. */
class NamedColumn[T: TypeMapper](val table: TableBase[_], _columnName: String, val options: ColumnOption[T]*) extends Column[T] {
  override val columnName = Some(_columnName)
  override def toString = "NamedColumn " + columnName
}

object NamedColumn {
  def unapply[T](n: NamedColumn[T]) = Some((n.table, n.columnName, n.options))
}