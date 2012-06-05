package revolute.query

import cascading.tuple.TupleEntry

import revolute.QueryException
import revolute.query.TypeMapper._
import revolute.util.{Identifier, NamingContext, Tuple}

import scala.collection._
import scala.sys.error

/** Common base trait for columns, tables and projections (but not unions and joins) */
trait ColumnBase[+T] extends Serializable {
  type _T = T

  /** Name hint for anonymous (e.g., calculated) column */
  def nameHint: String

  /** Columns (directly) required to compute this column */
  def dependencies: Set[ColumnBase[_]]

  def operationType: OperationType

  /** Returns a new EvaluationContext wrapping a parent evaluation context.
   *
   *  @see EvaluationChain
   */
  def chainEvaluation(parent: EvaluationContext): EvaluationContext
}

sealed trait OutputType

object OutputType {
  case object OneToZeroOrOne extends OutputType
  case object OneToMany      extends OutputType
  val values = List(OneToZeroOrOne, OneToMany)
}

trait SyntheticColumn[+T] extends ColumnBase[T] {
  override val nameHint = getClass.getSimpleName
  override val operationType = OperationType.PureMapper
  override def dependencies = sys.error("Should not be called; use case-by-case for synthetic column dependencies: " + this)
  override def chainEvaluation(context: EvaluationContext) = context // passthrough
}

/** Base class for columns */
abstract class Column[T: TypeMapper] extends ColumnBase[T] {
  final val typeMapper = implicitly[TypeMapper[T]]

  // def orElse(n: =>T): Column[T] = new WrappedColumn[T](this) { }

  // final def orFail = orElse { throw new QueryException("Read NULL value for column " + this) }

  def ? : Column[Option[T]] = error("todo") // new WrappedColumn[](this)(createOptionTypeMapper)

  def getOr[U](n: => U)(implicit ev: Option[U] =:= T): Column[U] = error("todo") // new WrappedColumn[U](this)(new BaseTypeMapper[U] {}) { }

  // def get[U](implicit ev: Option[U] =:= T): Column[U] = getOr[U] { throw new QueryException("Read NULL value for column "+this) }

  final def ~[U](b: ColumnBase[U])(implicit tm: TypeMapper[U]) = new Projection2[T, U](this, b)

  // Functions which don't need an OptionMapper
  // def in(e: Query[Column[_]]) = ColumnOps.In(this, e)
  // def notIn(e: Query[Column[_]]) = ColumnOps.Not(ColumnOps.In(this, e))
  def count = ColumnOps.Count(this)
  def isNull = ColumnOps.Is(this.asInstanceOf[Column[AnyRef]], ConstColumn.NULL)(NullOrdering)
  def isNotNull = ColumnOps.Not(ColumnOps.Is(this.asInstanceOf[Column[AnyRef]], ConstColumn.NULL)(NullOrdering))
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
case class ConstColumn[T : TypeMapper](val columnName: Option[String], value: T) extends Column[T] {
  override val nameHint = columnName.getOrElse("const-" + value)

  override val dependencies = Set.empty[ColumnBase[_]]

  override def operationType = OperationType.PureMapper

  override def chainEvaluation(context: EvaluationContext): EvaluationContext = {
    new EvaluationContext {
      val tuple = new Tuple { override def get(pos: Int) = value }
      override def nextTuple() = tuple
      override def position(c: ColumnBase[_]) = 0
      override def toString = ConstColumn.this.toString
    }
  }
  override def toString = value match {
    case null => "ConstColumn null"
    case a: AnyRef => "ConstColumn["+a.getClass.getName+"] "+a
    case _ => "ConstColumn "+value
  }
}

/** A column which gets created as the result of applying an operator. */
abstract class OperatorColumn[T : TypeMapper] extends Column[T] with ColumnOps[T, T] {
  override val nameHint = getClass.getSimpleName
  override def operationType: OperationType = OperationType.PureMapper
  val leftOperand: Column[T]
}

/** A column which is part of a Table. */
class NamedColumn[T: TypeMapper](val table: AbstractTable[_ <: Product], val columnName: String, val options: ColumnOption[_]*)
  extends OperatorColumn[T] with ColumnOps[T, T]
{
  val qualifiedColumnName = table.tableName + "." + columnName

  override val nameHint = qualifiedColumnName

  override def dependencies = Set.empty[ColumnBase[_]]

  override val leftOperand = this

  override val operationType = OperationType.PureMapper

  override def chainEvaluation(context: EvaluationContext): EvaluationContext = {
    new EvaluationContext {
      private[this] lazy val pos = context.position(NamedColumn.this)
      private[this] val tuple = new Tuple { override def get(pos: Int) = value }
      private[this] var value: Any = null
      override def nextTuple(): Tuple = {
        val next = context.nextTuple()
        if (next == null) return null
        value = next.get(pos)
        tuple
      }
      override def position(c: ColumnBase[_]) = 0
    }
  }

  override def equals(x: Any) = x match {
    case other: NamedColumn[_] => this.table == other.table && this.columnName == other.columnName
    case _ => false
  }
  override def hashCode = (table.hashCode * 13) + (columnName.hashCode * 17)
  override def toString = "NamedColumn(%s)" format qualifiedColumnName
}

object NullOrdering extends Ordering[AnyRef] {
  override def compare(x1: AnyRef, x2: AnyRef): Int = {
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

/** A column taken from the output of a Query. */
class QueryColumn[T: TypeMapper, C <: ColumnBase[T]](val query: Query[C], val column: C)
  extends OperatorColumn[C#_T] with ColumnOps[C#_T, C#_T]
{
  override val nameHint = column.nameHint

  override def dependencies = Set(query.value)

  override val leftOperand = this

  override val operationType = OperationType.PureMapper

  override def chainEvaluation(context: EvaluationContext): EvaluationContext = error("should not be called")
}