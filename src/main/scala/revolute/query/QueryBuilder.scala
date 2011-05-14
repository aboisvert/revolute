package revolute.query

import cascading.flow.{Flow, FlowConnector, FlowProcess}
import cascading.operation.{Filter, FilterCall, Identity, OperationCall}
import cascading.pipe.{CoGroup, Each, Pipe}
import cascading.tap.Tap
import cascading.tuple.Fields

import revolute.QueryException
import revolute.cascading._
import revolute.util._

import scala.collection._
import scala.collection.mutable.{HashMap, HashSet}


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

class BasicQueryBuilder[E <: ColumnBase[_]](_query: Query[E], _nc: NamingContext) {
  protected val query: Query[_] = _query
  protected implicit var nc: NamingContext = _nc

  final def build(): Pipe = {
    Console.println("build: value=%s" format _query.value)

    val tablePipes = {
      val tables = _query.tables
      if (tables.size == 1) {
        new Pipe(tables.head.tableName)
      } else {
        val pipes = tables map { t => new Pipe(t.tableName) }
        new CoGroup(pipes.toArray: _*)
      }
    }

    val pipe = new PipeBuilder(tablePipes)

    filters(_query.cond, pipe)
    select(_query.value, pipe)
    // _query.modifiers
    pipe.pipe
  }

  protected def select(value: Any, builder: PipeBuilder) {
    Console.println("select: value=%s" format value)
    value match {
      case nc: NamedColumn[_] =>
        builder += (new Each(_, new Fields(nc.columnName.get), new Identity(), Fields.RESULTS))

      case p: Projection[_] =>
        builder += (new Each(_, p.fields, new Identity(), Fields.RESULTS))

      case t: AbstractTable[_] =>
        builder += (new Each(_, t.*.fields, new Identity(), Fields.RESULTS))

      case c: Any =>
        innerExpr(c, builder)
    }
  }

  protected def filters(conditions: List[Column[_]], pipe: PipeBuilder) {
    Console.println("filter: conditions=%s" format conditions)
    conditions foreach { c => innerExpr(c, pipe) }
  }

  protected def expr(c: Any, b: PipeBuilder): Unit = {
    c match {
      case p: Projection[_] => {
        p.nodeChildren.foreach { c =>
          expr(c, b)
        }
      }
      case _ => innerExpr(c, b)
    }
  }

  protected def innerExpr(c: Any, pipe: PipeBuilder): Unit = {
    Console.println("innerExpr: " + c)
    c match {
      case ConstColumn(_, null) => error("todo")

      case ColumnOps.Not(ColumnOps.Is(l, ConstColumn(_, null))) => error("todo")

      case ColumnOps.Not(e) => error("todo")

      case ColumnOps.InSet(e, set, tm) =>
        Console.println("in set: %s %s" format (e, set))
        pipe += (new Each(_, new Fields(e.columnName.get), new InSetFilter(set)))

      case ColumnOps.Is(c: ColumnBase[_], v: ConstColumn[_]) =>
        Console.println("is: %s %s" format (c, v))
        pipe += (new Each(_, new Fields(c.columnName.get), new EqualConstFilter(v.value)))

      case ColumnOps.Is(l, ConstColumn(_, null)) => error("todo")

      case ColumnOps.Is(l, r) =>
        Console.println("is: %s %s" format (l, r))
        pipe += (new Each(_, new Fields(l.columnName.get, r.columnName.get), IsFilter))

      // case s: SimpleFunction => error("todo")

      // case s: SimpleScalarFunction => error("todo")

      case ColumnOps.Between(left, start, end) => error("todo")

      case ColumnOps.CountAll(q) => error("todo")

      case ColumnOps.CountDistinct(e) => error("todo")

      case ColumnOps.Regex(l, r) => error("todo")

      case a @ ColumnOps.AsColumnOf(ch, name) => error("todo")

      // case s: SimpleBinaryOperator => error("todo")

      case query:Query[_] => error("todo")

      case c @ ConstColumn(_, v) => error("todo")

      case n: NamedColumn[_] => error("todo")

      case t: AbstractTable[_] => expr(t.*, pipe)

      case t: TableBase[_] => error("todo")

      case concat @ ColumnOps.Concat(left, right) =>
        pipe += (new Each(_, new Fields(left.columnName.get, right.columnName.get), new ConcatOperation()))

      case _ => throw new QueryException("Don't know what to do with node \""+c+"\" in an expression")
    }
  }

  protected def appendConditions(b: PipeBuilder): Unit = query.cond match {
    case a :: l => error("todo")
    case Nil => ()
  }
}
