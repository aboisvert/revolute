package revolute.query

import cascading.flow.{Flow, FlowConnector}
import cascading.pipe.Pipe
import cascading.tap.Tap

import revolute.QueryException
import revolute.util.{Expr, NamingContext}
import revolute.query.StandardTypeMappers._

import scala.reflect.Manifest
import cascading.tuple.Fields

trait QueryVisitor[E <: ColumnBase[_], T] {
  def query(value: E, cond: List[Column[_]], modifiers: List[QueryModifier]): T
}

object Query extends Query(ConstColumn("UnitColumn", ()), Nil, Nil) {
  // def apply[E](value: ColumnBase[E]): Query[E] = new Query(value, Nil, Nil)
  // def apply[E: TypeMapper](value: E) = new Query(ConstColumn("foo", value), Nil, Nil)
}

/** Query monad: contains AST for query's projection, accumulated restrictions and other modifiers. */
class Query[E <: ColumnBase[_]](
  val value: E, 
  val cond: List[Column[_]],
  val modifiers: List[QueryModifier]
) extends ColumnBase[E#_T] {

  def visit[X](vis: QueryVisitor[E, X]) = vis.query(value, cond, modifiers)
  
  def flatMap[F <: ColumnBase[_]](f: E => Query[F]): Query[F] = {
    val q = f(value)
    new Query(q.value, cond ::: q.cond, modifiers ::: q.modifiers)
  }

  def map[F <: ColumnBase[_]](f: E => F): Query[F] = flatMap(v => new Query(f(v), Nil, Nil))

  def >>[F <: ColumnBase[F]](q: Query[F]): Query[F] = flatMap(_ => q)

  def filter[T <: ColumnBase[_]](f: E => T)(implicit wt: CanBeQueryCondition[T]): Query[E] =
    new Query(value, wt(f(value), cond), modifiers)

  def withFilter[T <: ColumnBase[_]](f: E => T)(implicit wt: CanBeQueryCondition[T]): Query[E] = filter(f)(wt)

  def where[T <: Column[_]](f: E => T)(implicit wt: CanBeQueryCondition[T]): Query[E] = filter(f)(wt)

  def groupBy(by: Column[_]*) =
    new Query[E](value, cond, modifiers ::: by.view.map(c => new Grouping(By(c))).toList)

  def orderBy(by: Ordering*) = new Query[E](value, cond, modifiers ::: by.toList)

  def exists = ColumnOps.Exists(map(_ => ConstColumn("exists", 1)))

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

  // def sub(implicit ev: E <:< ColumnBase[_]) = wrap(this)

  // Query[Column[_]] only
  //def asColumn(implicit ev: E <:< Column[_]): E = value
  
  def fields: Fields = {
    value match {
      case nc: NamedColumn[_] => new Fields(nc.columnName.get)
      case p: Projection[_] => p.fields
    }
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
      if(value) l else new ConstColumn(Some("BooleanCanBeQueryCondition"), false)(TypeMapper.BooleanTypeMapper) :: Nil
  }
}

/*
case class Subquery(query: Node, rename: Boolean) extends Node {
  def nodeChildren = query :: Nil
}

case class SubqueryColumn(pos: Int, subquery: Subquery, typeMapper: TypeMapper[_]) extends Node {
  def nodeChildren = subquery :: Nil
  override def toString = "SubqueryColumn c"+pos
}
*/

trait UnionVisitor[T] {
  def union(all: Boolean, queries: List[Query[_]]): T
}

case class Union[-V[X] <: UnionVisitor[X]](all: Boolean, queries: List[Query[_]]) extends Expr[V] {
  def accept[T](vis: V[T]): T = vis.union(all, queries)

  override def toString = if (all) "Union all" else "Union"
}

object QueryBuilder {
  implicit def queryToBuilder[T <: ColumnBase[_]](query: Query[T]) = new QueryBuilder(query)
}

class QueryBuilder[T <: ColumnBase[_]](val query: Query[T]) {
  def outputTo(sink: Tap)(implicit context: NamingContext): Flow = {
    val qb = new BasicQueryBuilder(query, NamingContext())
    val pipe = qb.build()
    val flow = new FlowConnector().connect(context.sources, sink, pipe)
    flow
  }
}

