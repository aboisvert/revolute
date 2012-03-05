package revolute.query

import revolute.QueryException

import scala.collection._
import scala.sys.error

/** Common base trait for columns, tables and projections (but not unions and joins) */
trait ColumnBase[+T] extends Serializable {
  type _T = T
  def arity: Int = arguments.size
  def arguments: Set[String]
  def columnName: Option[String] = None
  def evaluate(args: Map[String, Any]): T
  def tables: Set[AbstractTable[_]]
}

trait NotAnExpression extends ColumnBase[Nothing] {
  override def arguments = error("not supported")
  override def evaluate(args: Map[String, Any]): Nothing = error("not supported")
}

/** Base class for columns */
abstract class Column[T: TypeMapper] extends ColumnBase[T] {
  final val typeMapper = implicitly[TypeMapper[T]]

  // def orElse(n: =>T): Column[T] = new WrappedColumn[T](this) { }

  // final def orFail = orElse { throw new QueryException("Read NULL value for column " + this) }

  def ? : Column[Option[T]] = error("todo") // new WrappedColumn[](this)(createOptionTypeMapper)

  def getOr[U](n: => U)(implicit ev: Option[U] =:= T): Column[U] = error("todo") // new WrappedColumn[U](this)(new BaseTypeMapper[U] {}) { }

  def get[U](implicit ev: Option[U] =:= T): Column[U] = getOr[U] { throw new QueryException("Read NULL value for column "+this) }

  final def ~[U](b: Column[U]) = new Projection2[T, U](this, b)

  // Functions which don't need an OptionMapper
  // def in(e: Query[Column[_]]) = ColumnOps.In(this, e)
  // def notIn(e: Query[Column[_]]) = ColumnOps.Not(ColumnOps.In(this, e))
  def count = ColumnOps.Count(this)
  def isNull = ColumnOps.Is(this, ConstColumn.NULL)(NullOrdering)
  def isNotNull = ColumnOps.Not(ColumnOps.Is(this, ConstColumn.NULL)(NullOrdering))
  def countDistinct = ColumnOps.CountDistinct(this)

  def asc = new ResultOrdering.Asc(By(this))
  def desc = new ResultOrdering.Desc(By(this))
}

object ConstColumn {
  def NULL = apply[AnyRef]("NULL", null)(TypeMapper.NullTypeMapper)

  def apply[T : TypeMapper](columnName: String, value: T): ConstColumn[T] = ConstColumn(Some(columnName), value)
  def apply[T : TypeMapper](value: T): ConstColumn[T] = ConstColumn(None, value)
}

/** A column with a constant value (i.e., literal value) */
case class ConstColumn[T : TypeMapper](override val columnName: Option[String], value: T) extends Column[T] {
  override def tables = Set.empty[AbstractTable[_]]
  override val arguments = Set.empty[String]
  override def evaluate(args: Map[String, Any]): T = value
  override def toString = value match {
    case null => "ConstColumn null"
    case a: AnyRef => "ConstColumn["+a.getClass.getName+"] "+a
    case _ => "ConstColumn "+value
  }
}

/** A column which gets created as the result of applying an operator. */
abstract class OperatorColumn[T : TypeMapper] extends Column[T] with ColumnOps[T, T] {
  val leftOperand: ColumnBase[T] = this
}

/** A column which is part of a Table. */
class NamedColumn[T: TypeMapper](val table: AbstractTable[_], _columnName: String, val options: ColumnOption[_]*)
  extends OperatorColumn[T] with ColumnOps[T, T]
{
  override val columnName = Some(table.tableName + "." + _columnName)
  override val arguments = Set(columnName.get)
  override def evaluate(args: Map[String, Any]): T = args(columnName.get).asInstanceOf[T]
  override def tables = Set(table)

  override def equals(x: Any) = x match {
    case other: NamedColumn[_] => this.table == other.table && this.columnName == other.columnName
    case _ => false
  }
  override def hashCode = (table.hashCode * 13) + (columnName.hashCode * 17)
  override def toString = "NamedColumn(%s)" format columnName.get
}

object NullOrdering extends Ordering[Any] {
  override def compare(x1: Any, x2: Any): Int = {
    if ((x1.asInstanceOf[AnyRef] eq null) && (x2.asInstanceOf[AnyRef] eq null)) {
      return 0
    }
    if ((x1.asInstanceOf[AnyRef] ne null) && (x2.asInstanceOf[AnyRef] eq null)) {
      return -1
    }
    if ((x1.asInstanceOf[AnyRef] eq null) && (x2.asInstanceOf[AnyRef] ne null)) {
      return 1
    }
    return 0
  }
}
