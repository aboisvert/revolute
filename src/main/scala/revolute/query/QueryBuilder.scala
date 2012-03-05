package revolute.query

import cascading.flow.{Flow, FlowConnector, FlowProcess}
import cascading.operation.Identity
import cascading.pipe.{CoGroup, Each, Pipe}
import cascading.tuple.Fields

import revolute.QueryException
import revolute.cascading._
import revolute.util._
import revolute.util.Compat.Tap

import scala.sys.error

object QueryBuilder {
  implicit def queryToBuilder[T <: ColumnBase[_]](query: Query[T]) = new QueryBuilder(query)
}

class QueryBuilder[T <: ColumnBase[_]](val query: Query[T]) {
  def outputTo(sink: Tap)(implicit context: NamingContext, flowConnector: FlowConnector): Flow[_] = {
    val qb = new BasicQueryBuilder(query, NamingContext())
    val pipe = qb.build()
    val flow = flowConnector.connect(context.sources, sink, pipe)
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
        Console.println("select: named column; fields=%s" format nc.columnName.get)
        builder += (new Each(_, new Fields(nc.columnName.get), new Identity(), Fields.RESULTS))

      case p: Projection[_] =>
        Console.println("select: projection; source fields=%s" format p.sourceFields)
        if (p.columns.forall(_.isInstanceOf[NamedColumn[_]])) {
          // all named columns, just use identity transform
          builder += (new Each(_, p.sourceFields, new Identity(), Fields.RESULTS))
        } else {
          // at least one column is a transformation
          builder += (new Each(_, p.sourceFields, new MapOperation(p, p.fields), Fields.RESULTS))
        }

      case t: AbstractTable[_] =>
        Console.println("select: abstract table; source fields=%s" format t.*.sourceFields)
        builder += (new Each(_, t.*.sourceFields, new Identity(), Fields.RESULTS))

      case c: Any =>
        mapExpr(c, builder)
    }
  }

  protected def filters(conditions: List[Column[_]], pipe: PipeBuilder) {
    Console.println("filter: conditions=%s" format conditions)
    conditions foreach { c => filterExpr(c, pipe) }
  }

  protected def filterExpr(c: Any, pipe: PipeBuilder): Unit = {
    Console.println("filterExpr: " + c)
    c match {
      case ColumnOps.InSet(e, set) =>
        Console.println("in set: %s %s" format (e, set))
        pipe += (new Each(_, new Fields(e.columnName.get), new InSetFilter(set)))

      case ColumnOps.Is(c: ColumnBase[_], v: ConstColumn[_]) =>
        Console.println("is: %s %s" format (c, v))
        pipe += (new Each(_, new Fields(c.columnName.get), new EqualConstFilter(v.value)))

      case ColumnOps.Is(l, r) =>
        Console.println("is: %s %s" format (l, r))
        pipe += (new Each(_, new Fields(l.columnName.get, r.columnName.get), IsFilter))

      case expr: ColumnBase[_] if (!(expr.isInstanceOf[NotAnExpression])) =>
        Console.println("expression fields: " + expr.arguments)
        pipe += (new Each(_, new Fields(expr.arguments.toSeq: _*), new ExpressionFilter(expr.asInstanceOf[ColumnBase[Boolean]])))

      case _ => throw new QueryException("Don't know what to do with filter node \""+c+"\" in an expression")
    }
  }

  protected def mapExpr(c: Any, pipe: PipeBuilder): Unit = {
    Console.println("mapExpr: " + c)
    c match {
      case expr: ColumnBase[_] if (!(expr.isInstanceOf[NotAnExpression])) =>
        Console.println("map expression fields: " + expr.arguments)
        val args: Array[Comparable[_]] = expr.arguments.iterator.toArray
        pipe += (new Each(_, new Fields(args: _*), new MapSingleOperation(expr, nameFor(expr)), Fields.RESULTS))

      case _ => throw new QueryException("Don't know what to do with map node \""+c+"\" in an expression")
    }
  }

  protected def nameFor(column: ColumnBase[_]) = {
    column.columnName getOrElse { _nc.nameFor(column) }
  }

  protected def appendConditions(b: PipeBuilder): Unit = query.cond match {
    case a :: l => error("todo")
    case Nil => ()
  }
}
