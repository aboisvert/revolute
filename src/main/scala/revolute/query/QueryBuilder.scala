package revolute.query

import cascading.flow.{Flow, FlowConnector, FlowProcess}
import cascading.operation.Identity
import cascading.pipe.{CoGroup, Each, Pipe}
import cascading.tuple.Fields

import revolute.QueryException
import revolute.cascading._
import revolute.util._
import revolute.util.Compat._

import scala.sys.error

class QueryBuilder[E <: ColumnBase[_]](_query: Query[E], _nc: NamingContext) {
  protected val query: Query[_] = _query
  protected implicit var nc: NamingContext = _nc

  final def build(): Pipe = {
    val tablePipes = {
      val tables = EvaluationChain.tables(_query.value)
      Console.println("Pipe heads: " + tables)
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

  protected def select(value: Any, pipe: PipeBuilder) {
    import OperationType._

    Console.println("select: value=%s" format value)
    value match {
      case nc: NamedColumn[_] =>
        Console.println("select: named column; fields=%s" format nc.nameHint)
        pipe += (new Each(_, inputFields(nc), new Identity(), Fields.RESULTS))

      case p: Projection[_] =>
        Console.println("select: projection; columns=%s" format (p.columns))
        if (p.columns.forall(_.isInstanceOf[NamedColumn[_]])) {
          // all named columns, just use identity transform
          pipe += (new Each(_, inputFields(p), new Identity(), Fields.RESULTS))
        } else {
          Console.println("input fields: " + inputFields(p))
          Console.println("output fields: " + outputFields(p))
          pipe += (new Each(_, inputFields(p), new FlatMapOperation(p), Fields.RESULTS))
        }

      case t: AbstractTable[_] =>
        Console.println("select: abstract table; source fields=%s" format inputFields(t.*))
        pipe += (new Each(_, inputFields(t.*), new Identity(), Fields.RESULTS))

      case expr: Column[_] =>
        Console.println("map expression fields: " + expr.dependencies)
        Console.println("inputFields:  " + inputFields(expr))
        Console.println("outputFields: " + outputFields(expr))
        val p = new Projection1(expr)
        pipe += (new Each(_, inputFields(p), new FlatMapOperation(p), Fields.RESULTS))

      case _ => throw new QueryException("Don't know what to do with map node \"" + value + "\" in an expression")
    }
  }

  protected def filters(conditions: List[ColumnBase[_]], pipe: PipeBuilder) {
    Console.println("filter: conditions=%s" format conditions)
    conditions foreach { c => filterExpr(c, pipe) }
  }

  protected def filterExpr(c: Any, pipe: PipeBuilder): Unit = {
    Console.println("filterExpr: " + c)
    c match {
      case ColumnOps.InSet(e, set) =>
        Console.println("in set: %s %s" format (e, set))
        pipe += (new Each(_, inputFields(e), new InSetFilter(set)))

      case ColumnOps.Is(c: ColumnBase[_], v: ConstColumn[_]) =>
        Console.println("is: %s %s" format (c, v))
        pipe += (new Each(_, inputFields(c), new EqualConstFilter(v.value)))

      case ColumnOps.Is(l, r) =>
        Console.println("is: %s %s" format (l, r))
        pipe += (new Each(_, inputFields(new Projection2(l, r)), IsFilter))

      case expr: Column[_] =>
        Console.println("expression fields: " + expr.dependencies)
        pipe += (new Each(_, inputFields(expr), new ExpressionFilter(expr.asInstanceOf[Column[Boolean]])))

      case _ => throw new QueryException("Don't know what to do with filter node \""+c+"\" in an expression")
    }
  }

  protected def appendConditions(b: PipeBuilder): Unit = query.cond match {
    case a :: l => error("todo")
    case Nil => ()
  }

  private def inputFields(c: ColumnBase[_]) = EvaluationChain.inputFields(c)
  private def outputFields(c: ColumnBase[_]) = EvaluationChain.outputFields(c)
}
