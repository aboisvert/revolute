package revolute.flow

import cascading.flow.{Flow, FlowConnector, FlowProcess}
import cascading.pipe.Pipe
import revolute.query._
import revolute.util.NamingContext
import revolute.util.Compat._
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import revolute.cascading.EvaluationChain

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
  val sources = mutable.Map[TableBase[_], () => Tap]()
  val sinks   = mutable.Map[TableBase[_], () => Tap]()
}

object FlowBuilder {
  def flow(context: FlowContext)(f: FlowBuilder => Unit): Flow[_] = {
    val builder = new FlowBuilder(context)
    f(builder)
    val flow = builder.createFlow()
    Console.println("flow start()")
    flow.start()
    Console.println("flow complete()")
    flow.complete()
    Console.println("flow completed")
    flow
  }

  def sources(pipeNames: Map[TableBase[_], Set[String]], context: FlowContext): java.util.Map[String, Tap] = JavaConversions.mapAsJavaMap {
    pipeNames flatMap { case (table, names) =>
      names map { name =>
        val tapFactory = context.sources(table)
        name -> tapFactory.apply()
      }
    } toMap;
  }

}

class FlowBuilder(val context: FlowContext) {
  import FlowBuilder._

  private val statements = ArrayBuffer[Statement]()

  def insert[Q <: Query[_ <: ColumnBase[_]]](q: Q) = {
    new {
      def into[T <: Table](target: T): Insert[Q, T] = {
        val i = new Insert(q, target, context)
        statements += i
        i
      }
    }
  }

  def createFlow(): Flow[_] = {
    if (statements.size != 1) sys.error("FlowBuilder only supports 1 statement right now: " + statements.size)

    val statement = statements.head
    
    val pipes = mutable.Map[ColumnBase[_], Pipe]()
    val pipe = statement.pipe
    //Console.println()
    //Console.println("pipe: " + pipe)
    val sources = statement.sources
    Console.println("Sources: " + sources)
    val sink = statement.sink
    Console.println("Sink: " + sink)
    // val pipeNames = statements.head.pipe.getHeads.toSeq map (_.getName)
    // val srcs: java.util.Map[String, Tap] = sources(pipeNames, context)
    val flow = context.flowConnector.connect(JavaConversions.mapAsJavaMap(sources), sink, pipe)
    flow
  }
}

sealed trait Statement {
  def pipe: Pipe
  def sink: Tap
  def sources: Map[String, Tap]
}

class Insert[Q <: Query[_ <: ColumnBase[_]], T <: Table](val query: Q, val table: T, context: FlowContext) extends Statement {
  override def pipe = {
    val qb = new QueryBuilder(query.asInstanceOf[Query[query.QT]], NamingContext())
    qb.pipe
  }

  override def sink = {
    val result = context.sinks.getOrElse(table, sys.error("Missing sink binding for table %s" format table.tableName)).apply()
    //Console.println("sink: " + result)
    result
  }

  override def sources = {
    val srcs = mutable.Map[AbstractTable[_], () => Tap]()
    
    def visit(c: ColumnBase[_]) {
      //Console.println("sources visit %s" format c)
      c match { 
        case q: Query[_] => 
          q.subquery foreach visit
          visit(q.value)

        case _ => 
          val tables = EvaluationChain.tables(c)
          //Console.println
          // Console.println("sources -> queries: " + queries)
          // Console.println
          // Console.println("sources -> tables: " + tables)
          //Console.println
          tables foreach { t => 
            if (!srcs.contains(t) && context.sources.get(t).isDefined) {
              srcs += (t -> context.sources.get(t).get)
            }
          }
      }
    }
    
    visit(query)

    
    val result: Map[String, Tap] = srcs map { case (table, lazyTap) => table.tableName -> lazyTap.apply() } toMap;
    
    Console.println
    Console.println("sources:\n" + (result mkString "\n"))
    Console.println
    
    result
  }
}
