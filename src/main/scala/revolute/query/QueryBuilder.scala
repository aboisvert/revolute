package revolute.query

import cascading.flow.FlowProcess
import cascading.pipe.{CoGroup, Each, Pipe}
import cascading.operation.{Filter, FilterCall, Identity, OperationCall}
import cascading.tuple.Fields

import revolute.QueryException
import revolute.util._

import scala.collection._
import scala.collection.mutable.{HashMap, HashSet}

class BasicQueryBuilder[E <: ColumnBase[_]](_query: Query[E], _nc: NamingContext) 
  extends QueryVisitor[E, Pipe]
{
  protected val query: Query[_] = _query
  protected var nc: NamingContext = _nc

  override def query(value: E, cond: List[Column[_]], modifiers: List[QueryModifier]): Pipe = {
    val topLevel = _query.value match {
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
    // _query.modifiers
    pipe.pipe
  }

  protected def nameFor(t: TableBase[_]) = t match {
    case t: AbstractTable[_] => t.tableName
    case _ => _nc.nameFor(t)
  }
  
  final def build(): Pipe = {
    Console.println("build: value=%s" format _query.value)
    _query.visit(this)
  }
  
  protected def select(value: Any, builder: PipeBuilder) {
    Console.println("select: value=%s" format value)
    value match {
      case nc: NamedColumn[_] => builder += (new Each(_, new Fields(nc.columnName.get), new Identity(), Fields.RESULTS))
      case p: Projection[_] => builder += (new Each(_, p.fields, new Identity(), Fields.RESULTS))
    }
  }
  
  protected def filters(conditions: List[Column[_]], pipe: PipeBuilder) {
    Console.println("filter: conditions=%s" format conditions)
    conditions foreach { c => innerExpr(c, pipe) }
  }

  protected def expr(c: Any, b: PipeBuilder): Unit = expr(c, b, false, false)

  protected def expr(c: Any, b: PipeBuilder, rename: Boolean, topLevel: Boolean): Unit = {
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

  protected def innerExpr(c: Any, pipe: PipeBuilder): Unit = c match {
    case ConstColumn(_, null) => error("todo")
    case ColumnOps.Not(ColumnOps.Is(l, ConstColumn(_, null))) => error("todo")
    case ColumnOps.Not(e) => error("todo")
    case ColumnOps.InSet(e, seq, tm, bind) => error("todo")
    case ColumnOps.Is(c: ColumnBase[_], v: ConstColumn[_]) =>
      pipe += (new Each(_, new Fields(c.columnName getOrElse "TODO"), new EqualFilter(v.value)))
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
    // case SubqueryColumn(pos, sq, _) => error("todo")
    case t: AbstractTable[_] => expr(t.*, pipe)
    case t: TableBase[_] => error("todo")
    case _ => throw new QueryException("Don't know what to do with node \""+c+"\" in an expression")
  }

  protected def appendConditions(b: PipeBuilder): Unit = query.cond match {
    case a :: l => error("todo")
    case Nil => ()
  }

  protected def table(t: Any, name: String, b: PipeBuilder): Unit = t match {
    case base: AbstractTable[_] => error("todo")
    //case Subquery(sq: Query[_], rename) => error("todo")
    //case Subquery(Union(all, sqs), rename) => error("todo")
    case j: Join[_,_] => error("todo")
  }

  protected def createJoin(j: Join[_,_], b: PipeBuilder): Unit = {
    val l = j.left.asInstanceOf[TableBase[_]]  // FIXME
    val r = j.right.asInstanceOf[TableBase[_]] // FIXME
    table(l, nc.nameFor(l), b)
    // b += (new CoGroup(_, j.joinType))
    r match {
      case rj: Join[_,_] => createJoin(rj, b)
      case _ => table(r, nc.nameFor(r), b)
    }
    expr(j.on, b)
  }
}

abstract class ConstFilter(val value: Any) extends Filter[Any] with java.io.Serializable {
  def filter(x: Any): Boolean
  def isRemove(flowProcess: FlowProcess, filterCall: FilterCall[Any]) = filter(filterCall.getArguments.get(0))
  def isSafe = true
  def getNumArgs = 1
  def getFieldDeclaration = Fields.ALL
  def prepare(flowProcess: FlowProcess, operationCall: OperationCall[Any]) = ()
  def cleanup(flowProcess: FlowProcess, operationCall: OperationCall[Any]) = ()
}

class EqualFilter(_value: Any) extends ConstFilter(_value) {
  override def filter(x: Any) = (x != value)
}