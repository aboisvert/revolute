package revolute.query

import cascading.flow.{Flow, FlowConnector, FlowProcess}
import cascading.operation.Identity
import cascading.pipe.{CoGroup, Each, Pipe}
import cascading.pipe.joiner.MixedJoin
import cascading.tuple.Fields

import revolute.QueryException
import revolute.cascading._
import revolute.util._
import revolute.util.Compat._

import scala.collection._
import scala.sys.error


class QueryBuilder[E <: ColumnBase[_]](val query: Query[E], val nc: NamingContext, val distinctTableIds: DistinctMap[TableBase[_]] = new DistinctMap[TableBase[_]]()) {
  protected implicit var _: NamingContext = nc

  import QueryTypes.{Q, J}

  val pipe: Pipe = {
    Console.println("***")
    Console.println("Build: " + query)

    if (query.subquery.nonEmpty && query.joinQueries.nonEmpty) {
      sys.error("Can't have both joins and sub-queries")
    }
    
    val tables = EvaluationChain.tables(query.value)
    Console.println("tables: " + tables)

    /*
    val tablePipes = {
      Console.println("Pipe heads: " + tables)
      val pipes = tables.toSeq map { t => t -> new Pipe(tables.head.tableName) }
      Map(pipes: _*)
      /*
      if (tables.size == 1) {
        new Pipe(tables.head.tableName)
      } else {
        val pipes = tables map { t => new Pipe(t.tableName) }
        new CoGroup(pipes.toArray: _*)
      }*/
    }
    */

    // validate joins/table usage
    val joins  : Seq[J] = query.modifiers collect { case j: J => j }
    
    val headPipe = if (joins.size == 0) {
      if (query.subquery.isDefined) {
        new QueryBuilder(query.subquery.get, nc, distinctTableIds).pipe
      } else { 
        tables.size match {
          case 0 => sys.error("No joins or tables specified in query")
          case 1 => new Pipe(distinctTableIds.distinctId(tables.head, tables.head.tableName))
          case n if n >= 2 => sys.error("Join required when using two or more tables in a query")
        }
      }
      
    } else {
      Console.println("linearized join queries: " + query.joinQueries)
      
      val joinQueries = query.joinQueries
      
      val left = joins(0).left
      joins foreach { j =>
        if (j.left != left) sys.error("The left side of all joins should use the same table/query")
        if (j.left == j.right) sys.error("A table/query cannot be joined with itself")
      }
      val allTables = {
        val joined: Seq[_ <: ColumnBase[_]] = joins flatMap { j => Seq(j.left.value, j.right.value) }
        joined flatMap (EvaluationChain.tables) toSet
      }
      Console.println("tables: " + tables)
      Console.println("allTables: " + allTables)

      tables foreach { t =>
        if (! allTables.contains(t)) sys.error("Table/query %s not part of join clause" format t)
      }

      /*
      val pipes = queryBuilders.values map (_.pipe)
      Console.println("pipes: " + pipes)
      */

      val groupFields = {
        val columns = joinQueries map { q =>
          val join = joins find { j => (j.left == q) || (j.right == q) } get;
          join.on match {
            case ColumnOps.Is(left, right) => if (q == join.left) left else right
            case unsupported => sys.error("Unsupported join operator: " + unsupported)
          }
        }
        columns map { c => outputFields(c) }
      }
      Console.println("groupFields: " + groupFields)

      val (distinctColumnNames, declaredFields) = {
        val defaultColumnNames = joinQueries flatMap { q =>
          val columns = outputColumns(q.value)
          val names = outputNames(q.value)
          columns zip names
        }
        val distinctColumnNames = {
          val distinct = new DistinctNames()
          defaultColumnNames map { case (column, name) => column -> distinct.distinctName(name) }
        }
        val fields = {
          val names = distinctColumnNames map { case (column, name) => name }
          new Fields(names: _*)
        }
        (distinctColumnNames.toMap, Fields.join(fields))
      }
      Console.println("declaredFields: " + declaredFields)

      val joinTypes = Seq(true) ++ (joins map (_.joinType == Join.Inner))
      Console.println("join types: " + joinTypes)

      val pipes = joinQueries map { q => new QueryBuilder(q, nc, distinctTableIds).pipe }
      
      new CoGroup(pipes.toArray, groupFields.toArray, declaredFields, new MixedJoin(joinTypes.toArray))
    }

    val pipe = new PipeBuilder(headPipe)

    filters(query.cond, pipe)
    select(query.value, pipe)
    // _query.modifiers
    Console.println("---" + query)
    Console.println("==>" + pipe.pipe)
    pipe.pipe
  }

  protected def select(value: Any, pipe: PipeBuilder) {
    import OperationType._

    Console.println("select: value=%s" format value)
    value match {
      case nc: NamedColumn[_] =>
        Console.println("select: named column; fields=%s" format nc.nameHint)
        pipe += (new Each(_, inputFields(nc), new Identity(), Fields.RESULTS))

      case t: AbstractTable[_] =>
        Console.println("select: abstract table; source fields=%s" format inputFields(t.*))
        pipe += (new Each(_, inputFields(t.*), new Identity(), Fields.RESULTS))

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
    //Console.println("filterExpr: " + c)
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
  private def outputColumns(c: ColumnBase[_]) = EvaluationChain.outputColumns(c)
  private def outputNames(c: ColumnBase[_]) = EvaluationChain.outputNames(c)

}
