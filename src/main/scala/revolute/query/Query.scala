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
  val cond: List[ColumnBase[_]],
  val modifiers: List[QueryModifier]
) extends SyntheticColumn[E#_T] {

  override val nameHint = "Query(%s)" format value.nameHint

  def flatMap[F <: ColumnBase[_]](f: E => Query[F]): Query[F] = {
    val q = f(value)
    new Query(q.value, cond ::: q.cond, modifiers ::: q.modifiers)
  }

  def map[F <: ColumnBase[_]](f: E => F): Query[F] = flatMap(v => new Query(f(v), Nil, Nil))

  def >>[F <: ColumnBase[F]](q: Query[F]): Query[F] = flatMap(_ => q)

  def filter[T](f: E => T)(implicit wt: CanBeQueryCondition[T]): Query[E] =
    new Query(value, wt(f(value), cond), modifiers)

  def withFilter[T](f: E => T)(implicit wt: CanBeQueryCondition[T]): Query[E] = filter(f)(wt)

  def where[T](f: E => T)(implicit wt: CanBeQueryCondition[T]): Query[E] = filter(f)(wt)

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

  // Query[ColumnBase[_]] only
  def union[O >: E <: ColumnBase[_]](other: Query[O]*) = Union(false, this :: other.toList)

  def unionAll[O >: E <: ColumnBase[_]](other: Query[O]*) = Union(true, this :: other.toList)

  def count(implicit ev: E <:< ColumnBase[_]) = ColumnOps.CountAll(value)

  override def toString = "Query(value=%s, cond=%s, modifiers=%s)" format (value, cond, modifiers)
}

trait CanBeQueryCondition[-T] {
  def apply(value: T, l: List[ColumnBase[_]]): List[ColumnBase[_]]
}

object CanBeQueryCondition {

  implicit object BooleanColumnCanBeQueryCondition extends CanBeQueryCondition[ColumnBase[Boolean]] {
    override def apply(value: ColumnBase[Boolean], l: List[ColumnBase[_]]): List[ColumnBase[_]] = value :: l
  }

  implicit object BooleanOptionColumnCanBeQueryCondition extends CanBeQueryCondition[ColumnBase[Option[Boolean]]] {
    override def apply(value: ColumnBase[Option[Boolean]], l: List[ColumnBase[_]]): List[ColumnBase[_]] = value :: l
  }

  implicit object BooleanCanBeQueryCondition extends CanBeQueryCondition[Boolean] {
    override def apply(value: Boolean, l: List[ColumnBase[_]]): List[ColumnBase[_]] =
      if (value) l else new ConstColumn(Some("BooleanCanBeQueryCondition"), false)(TypeMapper.BooleanTypeMapper) :: Nil
  }

  implicit def tuple2CanBeQueryCondition[T1, T2](p: Projection2[T1, T2]) = new Tuple2CanBeQueryCondition[T1, T2]

  class Tuple2CanBeQueryCondition[T1, T2] extends CanBeQueryCondition[Projection2[T1, T2]] {
    override def apply(value: Projection2[T1, T2], l: List[ColumnBase[_]]): List[ColumnBase[_]] = List(value._1, value._2)
  }
}

case class Union(all: Boolean, queries: List[Query[_]]) {
  override def toString = if (all) "Union all" else "Union"
}
