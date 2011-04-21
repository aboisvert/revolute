package revolute.query

import cascading.flow.FlowProcess
import cascading.pipe.{CoGroup, Each, Pipe}
import cascading.operation.{Filter, FilterCall, Identity, OperationCall}
import cascading.tuple.Fields

import revolute.QueryException
import revolute.util._

import scala.collection._
import scala.collection.mutable.{HashMap, HashSet}

class ConcreteBasicQueryBuilder(
  _query: Query[_], 
  _nc: NamingContext, 
  parent: Option[BasicQueryBuilder]
) extends BasicQueryBuilder(_query, _nc, parent) {
  type Self = BasicQueryBuilder

  protected def createSubQueryBuilder(query: Query[_], nc: NamingContext) =
    new ConcreteBasicQueryBuilder(query, nc, Some(this))
}

abstract class BasicQueryBuilder(_query: Query[_], _nc: NamingContext, parent: Option[BasicQueryBuilder]) {
  type Self <: BasicQueryBuilder

  protected def createSubQueryBuilder(query: Query[_], nc: NamingContext): Self

  protected val query: Query[_] = _query
  protected var nc: NamingContext = _nc
  protected val localTables = new HashMap[String, Node]
  protected val declaredTables = new HashSet[String]
  protected val subQueryBuilders = new HashMap[RefId[Query[_]], Self]

  protected def localTableName(n: Node) = n match {
    case Join.JoinPart(table, from) =>
      // Special case for Joins: A join combines multiple tables but does not alias them
      localTables(nc.nameFor(from)) = from
      nc.nameFor(table)
    case _ =>
      val name = nc.nameFor(n)
      localTables(name) = n
      name
  }

  protected def isDeclaredTable(name: String): Boolean =
    if (declaredTables contains name) true
    else parent map (_.isDeclaredTable(name)) getOrElse (false)
  
  protected def subQueryBuilderFor(q: Query[_]) =
    subQueryBuilders.getOrElseUpdate(new RefId(q), createSubQueryBuilder(q, nc))

  final def build(): Pipe = {
    Console.println("build: value=%s" format _query.value)
    
    def nameFor(t: TableBase[_]) = t match {
      case t: AbstractTable[_] => t.tableName
      case _ => _nc.nameFor(t)
    }
    
    // top-level pipes
    var topLevel = _query.value match {
      case nc: NamedColumn[_] =>
        new Pipe(nameFor(nc.table))
        
      case p: Projection[_] =>
        val tables = p.columns.foldLeft(Set[TableBase[_]]()) { case (tables, c) => 
          c match { 
            case n: NamedColumn[_] => tables + n.table
            case _ => tables
          }
        }
        Console.println("tables: %s" format tables)
        if (tables.size == 1) {
          new Pipe(nameFor(tables.head))
        } else {
          val pipes = tables map { t => new Pipe(nameFor(t)) }
          new CoGroup(pipes.toArray: _*)
        }
    }
    
    val pipe = new PipeBuilder(topLevel)

    select(_query.value, pipe)
    filters(_query.cond, pipe)
    // _query.condHaving
    // _query.modifiers
    pipe.pipe
  }
  
  protected def select(value: Any, builder: PipeBuilder) {
    Console.println("select: value=%s" format value)
    value match {
      case nc: NamedColumn[_] => builder += (new Each(_, new Fields(nc.columnName), new Identity(), Fields.RESULTS))
      case p: Projection[_] => builder += (new Each(_, p.fields, new Identity(), Fields.RESULTS))
    }
  }
  
  protected def filters(conditions: List[Column[_]], pipe: PipeBuilder) {
    Console.println("filter: conditions=%s" format conditions)
    conditions foreach { c => innerExpr(c, pipe) }
  }

  protected def expr(c: Node, b: PipeBuilder): Unit = expr(c, b, false, false)

  protected def expr(c: Node, b: PipeBuilder, rename: Boolean, topLevel: Boolean): Unit = {
    c match {
      case p: Projection[_] => {
        p.nodeChildren.foreach { c =>
          expr(c, b, false, true)
          if (rename) { /* newName */ }
        }
      }
      case _ => innerExpr(c, b)
    }
  }

  protected def innerExpr(c: Node, pipe: PipeBuilder): Unit = c match {
    case ConstColumn(_, null) => error("todo")
    case ColumnOps.Not(ColumnOps.Is(l, ConstColumn(_, null))) => error("todo")
    case ColumnOps.Not(e) => error("todo")
    case ColumnOps.InSet(e, seq, tm, bind) => error("todo")
    case ColumnOps.Is(c: ColumnBase[_], v: ConstColumn[_]) =>
      pipe += (new Each(_, new Fields(c.columnName), new ConstFilter(v.value)))
    case ColumnOps.Is(l, ConstColumn(_, null)) => error("todo")
    case ColumnOps.Is(l, r) => error("todo")
    case s: SimpleFunction => error("todo")
    case s: SimpleScalarFunction => error("todo")
    case ColumnOps.Between(left, start, end) => error("todo")
    case ColumnOps.CountAll(q) => error("todo")
    case ColumnOps.CountDistinct(e) => error("todo")
    case ColumnOps.Regex(l, r) => error("todo")
    case a @ ColumnOps.AsColumnOf(ch, name) => error("todo")
    case s: SimpleBinaryOperator => error("todo")
    case query:Query[_] => error("todo")
    case c @ ConstColumn(_, v) => error("todo")
    case n: NamedColumn[_] => error("todo")
    case SubqueryColumn(pos, sq, _) => error("todo")
    case t: AbstractTable[_] => expr(Node(t.*), pipe)
    case t: TableBase[_] => error("todo")
    case _ => throw new QueryException("Don't know what to do with node \""+c+"\" in an expression")
  }

  protected def appendConditions(b: PipeBuilder): Unit = query.cond match {
    case a :: l => error("todo")
    case Nil => ()
  }

  protected def table(t: Node, name: String, b: PipeBuilder): Unit = t match {
    case base: AbstractTable[_] => error("todo")
    case Subquery(sq: Query[_], rename) => error("todo")
    case Subquery(Union(all, sqs), rename) => error("todo")
    case j: Join[_,_] => error("todo")
  }

  protected def createJoin(j: Join[_,_], b: PipeBuilder): Unit = {
    val l = j.leftNode
    val r = j.rightNode
    table(l, nc.nameFor(l), b)
    // b += (new CoGroup(_, j.joinType))
    r match {
      case rj: Join[_,_] => createJoin(rj, b)
      case _ => table(r, nc.nameFor(r), b)
    }
    expr(j.on, b)
  }

  protected def untupleColumn(columns: Node) = {
    val l = new scala.collection.mutable.ListBuffer[Node]
    def f(c: Any): Unit = c match {
      case p: Projection[_] =>
        for (i <- 0 until p.productArity)
          f(Node(p.productElement(i)))
      case t: AbstractTable[_] => f(Node(t.*))
      case n: Node => l += n
      case v => throw new QueryException("Cannot untuple non-Node value " + v)
    }
    f(Node(columns))
    l.toList
  }
}

class ConstFilter(val value: Any) extends Filter[Any] with java.io.Serializable {
  def isRemove(flowProcess: FlowProcess, filterCall: FilterCall[Any]) = filterCall.getArguments.get(0) != value
  def isSafe = true
  def getNumArgs = 1
  def getFieldDeclaration = Fields.ALL
  def prepare(flowProcess: FlowProcess, operationCall: OperationCall[Any]) = ()
  def cleanup(flowProcess: FlowProcess, operationCall: OperationCall[Any]) = ()
}
