package revolute.query

import revolute.QueryException
import revolute.util.NamingContext
import revolute.query.StandardTypeMappers._

import scala.reflect.Manifest
import cascading.tuple.Fields

object Query extends Query(ConstColumn("UnitColumn", ()), Nil, Nil) {
}

/** Query monad: contains AST for query's projection, accumulated restrictions and other modifiers. */
class Query[E <: ColumnBase[_]](
  val value: E,
  val cond: List[Column[_]],
  val modifiers: List[QueryModifier]
) extends ColumnBase[E#_T] with NotAnExpression {

  def flatMap[F <: ColumnBase[_]](f: E => Query[F]): Query[F] = {
    val q = f(value)
    new Query(q.value, cond ::: q.cond, modifiers ::: q.modifiers)
  }

  /*
  def flatMap2[F](f: E => Option[F]): Query[F] = {
    val q = f(value)
    val col = new Column[F] {}
    new Query(col, cond ::: q.cond, modifiers ::: q.modifiers)
    error("todo")
  }
   */

  def map[F <: ColumnBase[_]](f: E => F): Query[F] = flatMap(v => new Query(f(v), Nil, Nil))

  def >>[F <: ColumnBase[F]](q: Query[F]): Query[F] = flatMap(_ => q)

  def filter[T <: ColumnBase[_]](f: E => T)(implicit wt: CanBeQueryCondition[T]): Query[E] =
    new Query(value, wt(f(value), cond), modifiers)

  def withFilter[T <: ColumnBase[_]](f: E => T)(implicit wt: CanBeQueryCondition[T]): Query[E] = filter(f)(wt)

  def where[T <: Column[_]](f: E => T)(implicit wt: CanBeQueryCondition[T]): Query[E] = filter(f)(wt)

  def groupBy(by: Column[_]*) =
    new Query[E](value, cond, modifiers ::: by.view.map(c => new Grouping(By(c))).toList)

  def orderBy(by: ResultOrdering*) = new Query[E](value, cond, modifiers ::: by.toList)

  //def exists = ColumnOps.Exists(map(_ => ConstColumn("exists", 1)))

  def typedModifiers[T <: QueryModifier](implicit m: ClassManifest[T]) =
    modifiers.filter(m.erasure.isInstance(_)).asInstanceOf[List[T]]

  def createOrReplaceSingularModifier[T <: QueryModifier](f: Option[T] => T)(implicit m: Manifest[T]): Query[E] = {
    val (xs, other) = modifiers.partition(m.erasure.isInstance(_))
    val mod = xs match {
      case x :: _ => f(Some(x.asInstanceOf[T]))
      case _ => f(None)
    }
    new Query[E](value, cond, mod :: other)
  }

  override def tables = value.tables ++ cond.flatMap(_.tables)

  // Query[ColumnBase[_]] only
  def union[O >: E <: ColumnBase[_]](other: Query[O]*) = Union(false, this :: other.toList)

  def unionAll[O >: E <: ColumnBase[_]](other: Query[O]*) = Union(true, this :: other.toList)

  def count(implicit ev: E <:< ColumnBase[_]) = ColumnOps.CountAll(value)

  // def sub(implicit ev: E <:< ColumnBase[_]) = wrap(this)

  def fields(implicit context: NamingContext): Fields = {
    Console.println("Query.fields on %s" format value)


    val f = value match {
      case nc: NamedColumn[_] => new Fields(nc.columnName.get)
      case p: Projection[_] => p.fields
      case _ => Fields.ALL
    }
    Console.println("Query.fields: %s" format f)
    f
  }

  override def toString = "Query"
}

trait CanBeQueryCondition[-T] {
  def apply(value: T, l: List[Column[_]]): List[Column[_]]
}

object CanBeQueryCondition {
  implicit object BooleanColumnCanBeQueryCondition extends CanBeQueryCondition[Column[Boolean]] {
    def apply(value: Column[Boolean], l: List[Column[_]]): List[Column[_]] = value :: l
  }
  implicit object BooleanOptionColumnCanBeQueryCondition extends CanBeQueryCondition[Column[Option[Boolean]]] {
    def apply(value: Column[Option[Boolean]], l: List[Column[_]]): List[Column[_]] = value :: l
  }
  implicit object BooleanCanBeQueryCondition extends CanBeQueryCondition[Boolean] {
    def apply(value: Boolean, l: List[Column[_]]): List[Column[_]] =
      if (value) l else new ConstColumn(Some("BooleanCanBeQueryCondition"), false)(TypeMapper.BooleanTypeMapper) :: Nil
  }
}

case class Union(all: Boolean, queries: List[Query[_]]) {
  override def toString = if (all) "Union all" else "Union"
}
