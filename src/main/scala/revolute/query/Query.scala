package revolute.query

import revolute.QueryException
import revolute.util.NamingContext
import revolute.query.StandardTypeMappers._

import scala.reflect.Manifest
import cascading.tuple.Fields

trait Joinable[T <: ColumnBase[_]] {
  def asQuery(t: T): Query[T]
}

object QueryTypes {
  type Q = Query[_ <: ColumnBase[_]]
  type J = Join[_ <: Q, _ <: Q]
}

import QueryTypes._

object Query extends Query(ConstColumn("UnitColumn", ()), None, Nil, Nil) {
}

/** Query monad: contains AST for query's projection, accumulated restrictions and other modifiers.
 *
 *  A query represents a transformative sequence of,
 *    1) one or more table joined or merged together,
 *    2) field selection and function application (e.g. select clause, incl. 1-1, 1-0/1, 1-N mapping)
 *    3) one or more filters (e.g. where clause)
 *    4) grouping and sorting (e.g. group-by / sort-by)
 *    5) aggregation based on groupings.
 */
class Query[E <: ColumnBase[_]](
  val value: E,
  val subquery: Option[Q],
  val cond: List[ColumnBase[_]],
  val modifiers: List[QueryModifier]
) extends SyntheticColumn[E] {
  type QT = E
  
  //Console.println("new query: " + value)

  {
    // val e = new Exception("new query: " + value)
    //e.printStackTrace
  }

  override val nameHint = "Query(%s)" format value.nameHint

  def flatMap[F <: ColumnBase[_]](f: E => Query[F]): Query[F] = {
    val q = f(value)
    // Console.println("Flatmap %s with %s" format (this, q))
    if (q != value) {
      new Query(q.value, Some(q), cond ::: q.cond, modifiers ::: q.modifiers)
    } else {
      this.asInstanceOf[Query[F]]
    }
  }

  def map[F <: ColumnBase[_]](f: E => F): Query[F] = {
    val newValue = f(value)
    if (newValue != value) {
      new Query(newValue, Some(this), cond, modifiers)
    } else {
      this.asInstanceOf[Query[F]]
    }
  }

  def >>[F <: ColumnBase[F]](q: Query[F]): Query[F] = flatMap(_ => q)

  def filter[T](f: E => T)(implicit wt: CanBeQueryCondition[T]): Query[E] = {
    //Console.println("(%s).filter(%s)(%s)" format (this, f, wt))
    val newModifiers = value match {
      case j: Join[_, _] => modifiers :+ j
      case other         => modifiers
    }
    new Query(value, subquery, wt(f(value), cond), newModifiers)
  }

  def withFilter[T](f: E => T)(implicit wt: CanBeQueryCondition[T]): Query[E] = filter(f)(wt)

  def where[T](f: E => T)(implicit wt: CanBeQueryCondition[T]): Query[E] = filter(f)(wt)

  def groupBy(by: Column[_]*) =
    new Query[E](value, subquery, cond, modifiers ::: by.view.map(c => new Grouping(By(c))).toList)

  def orderBy(by: ResultOrdering*) = new Query[E](value, subquery, cond, modifiers ::: by.toList)

  //def exists = ColumnOps.Exists(map(_ => ConstColumn("exists", 1)))

  def typedModifiers[T <: QueryModifier](implicit m: ClassManifest[T]) =
    modifiers.filter(m.erasure.isInstance(_)).asInstanceOf[List[T]]

  def createOrReplaceSingularModifier[T <: QueryModifier](f: Option[T] => T)(implicit m: Manifest[T]): Query[E] = {
    val (xs, other) = modifiers.partition(m.erasure.isInstance(_))
    val mod = xs match {
      case x :: _ => f(Some(x.asInstanceOf[T]))
      case _ => f(None)
    }
    new Query[E](value, dependencies, cond, mod :: other)
  }

  // Query[ColumnBase[_]] only
  def union[O >: E <: ColumnBase[_]](other: Query[O]*) = Union(false, this :: other.toList)

  def unionAll[O >: E <: ColumnBase[_]](other: Query[O]*) = Union(true, this :: other.toList)

  def count(implicit ev: E <:< ColumnBase[_]) = ColumnOps.CountAll(value)

  override def toString = {
    if (subquery.isEmpty) {
      "Query(\n  value=%s,\n  cond=%s,\n  modifiers=%s)" format (value, cond, modifiers)
    } else {
      "Query(\n  value=%s,\n  cond=%s,\n  modifiers=%s,\n  subquery=\n%s\n)" format (value, cond, modifiers, ASTPrinter.nested(subquery.toString))
    }
  }

  def innerJoin[C <: ColumnBase[_]](other: C)(implicit joinable: Joinable[C]) : JoinBase[QT, C] =
    new JoinBase[QT, C](this, joinable.asQuery(other), Join.Inner)
    
  private[query] def joinQueries = {
    val joins  : Seq[J] = modifiers collect { case j: J => j }
    val queries: Seq[Q] = joins map { j => Seq(j.left, j.right) } flatten;
    queries.distinct
  }    
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
    override def apply(value: Boolean, l: List[ColumnBase[_]]): List[ColumnBase[_]] = {
      //Console.println("BooleanCanBeQueryCondition.apply(%s, %s)" format (value, l))
      if (value) l else new ConstColumn(Some("BooleanCanBeQueryCondition"), false)(TypeMapper.BooleanTypeMapper) :: Nil
    }
  }

  implicit def tuple2CanBeQueryCondition[T1, T2](p: Projection2[T1, T2]) = new Tuple2CanBeQueryCondition[T1, T2]

  class Tuple2CanBeQueryCondition[T1, T2] extends CanBeQueryCondition[Projection2[T1, T2]] {
    override def apply(value: Projection2[T1, T2], l: List[ColumnBase[_]]): List[ColumnBase[_]] = List(value._1, value._2)
  }
}

case class Union(all: Boolean, queries: List[Query[_]]) {
  override def toString = if (all) "Union all" else "Union"
}

object ASTPrinter {
  final val threadLocal = new ThreadLocal[Int]()

  
  def nested(s: => String) = {
    val depth = Option(threadLocal.get) getOrElse 1
    val prefix = ("|    " * depth)
    threadLocal.set(depth+1)
    try {
      val orig = s
      prefix + orig.replaceAll("\n", "\n" + prefix)
    } finally {
      threadLocal.set(depth)
    }
  }
}