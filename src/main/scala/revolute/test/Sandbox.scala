package revolute.test

import cascading.flow.Flow
import cascading.tap.SinkMode
import cascading.tap.local.FileTap
import cascading.tuple.Tuple
import cascading.scheme.local.TextDelimited

import revolute.cascading._
import revolute.flow._
import revolute.query._
import revolute.query.ImplicitConversions._
import revolute.util._
import revolute.util.Compat._
import revolute.util.Converters._

import scala.collection._
import scala.collection.mutable.ArrayBuffer

object Sandbox {
  def getTuples(flow: Flow[_]): Seq[Tuple] = {
    val iterator = flow.openSink()
    val result = ArrayBuffer[Tuple]()
    while (iterator.hasNext) {
      result += iterator.next.getTupleCopy()
    }
    iterator.close()
    result
  }
}

class Sandbox(val outputDir: String) {

  private var outputNumber = 0

  def outputFile(file: String) = synchronized {
    outputNumber += 1
    file + "-" + outputNumber
  }

  def run[T](query: Query[_ <: ColumnBase[T]])(implicit context: FlowContext): Seq[Tuple] = {
    implicit val _ = context.namingContext

    Console.println()
    Console.println()
    Console.println()
    Console.println("query: " + query)
    val qb = new QueryBuilder(query, NamingContext())
    val pipe = qb.pipe
    Console.println("pipe: " + pipe)

    val tables = qb.distinctTableIds.distinctIds // EvaluationChain.tables(query.value) map (_.tableName)
    val sources = FlowBuilder.sources(tables, context)
    Console.println("sources: " + sources)
    //Console.println("<sources>\n" + (sources mkString "\n"))
    //Console.println("</sources>")
    val sink = new FileTap(new TextDelimited(outputFields(query.value), "\t"), outputFile(outputDir), SinkMode.REPLACE)
    Console.println("sink: " + sink)
    val flow = context.flowConnector.connect(sources, sink, pipe)
    flow.start()
    flow.complete()
    Console.println("completed query: " + query)
    Console.println()
    Console.println()
    Console.println()
    Sandbox.getTuples(flow)
  }
}
