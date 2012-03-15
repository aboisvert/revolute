package revolute.flow

import cascading.flow.{Flow, FlowConnector, FlowProcess}
import cascading.pipe.Pipe

import revolute.query.{ColumnBase, Query, QueryBuilder, Table}
import revolute.util.NamingContext
import revolute.util.Compat._

import scala.collection._
import scala.collection.mutable.ArrayBuffer

object FlowContext {
  def local(f: FlowContext => Unit): FlowContext = {
    apply(flowConnector = new cascading.flow.local.LocalFlowConnector())(f)
  }

  def apply(namingContext: NamingContext = NamingContext(), flowConnector: FlowConnector)(f: FlowContext => Unit): FlowContext = {
    val context = new FlowContext(namingContext, flowConnector)
    f(context)
    context
  }
}

class FlowContext(val namingContext: NamingContext, val flowConnector: FlowConnector) {
  val sources = mutable.Map[Table[_], Tap]()
  val sinks   = mutable.Map[Table[_], Tap]()
}

object FlowBuilder {
  def flow(context: FlowContext)(f: FlowBuilder => Unit): Flow[_] = {
    val builder = new FlowBuilder(context)
    f(builder)
    val flow = builder.createFlow()
    flow.start()
    flow.complete()
    flow
  }
}

class FlowBuilder(val context: FlowContext) {

  private val statements = ArrayBuffer[Statement]()

  def insert[Q <: Query[ColumnBase[_]]](q: Q) = {
    new {
      def into[T <: Table[_]](t: T): Insert[Q, T] = {
        val i = new Insert(q, t, context)
        statements += i
        i
      }
    }
  }

  def createFlow(): Flow[_] = {
    if (statements.size != 1) sys.error("FlowBuilder only supports 1 statement right now")

    val pipe = statements.head.pipe
    val sink = statements.head.sink
    val sources: java.util.Map[String, Tap] = JavaConversions.mapAsJavaMap {
      context.sources map { case (table, tap) => (table.tableName, tap: Tap) } toMap
    }
    val flow = context.flowConnector.connect(sources, sink, pipe)
    flow
  }
}

sealed trait Statement {
  def pipe: Pipe
  def sink: Tap
}

class Insert[Q <: Query[ColumnBase[_]], T <: Table[_]](val query: Q, val table: T, context: FlowContext) extends Statement {
  def pipe = {
    val qb = new QueryBuilder(query, NamingContext())
    val pipe = qb.build()
    pipe
  }

  def sink = {
    context.sinks.getOrElse(table, sys.error("Missing sink binding for table %s" format table.tableName))
  }
}
