package revolute.cascading

import cascading.flow.FlowProcess
import cascading.operation.{BaseOperation, Function, FunctionCall}
import cascading.tuple.{Fields, Tuple}

import revolute.query.{ColumnBase, Projection}
import revolute.query.OperationType._
import revolute.util.Combinations

import scala.collection._
import scala.collection.mutable.ArrayBuffer

sealed trait OutputType

object OutputType {
  case object OneToZeroOrOne
  case object OneToMany

  val values = List(OneToZeroOrOne, OneToMany)
}

class FlatMapOperation(val projection: Projection[_], val fields: Fields)
  extends BaseOperation[Any](projection.columns.size, fields) with Function[Any]
{
  import OutputType._

  lazy val outputType = {
    val operationTypes = projection.columns map (_.operationType)
    if (operationTypes exists (_ == SeqMapper)) OneToZeroOrOne else OneToMany
  }

  override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[Any]) {
    val args = functionCall.getArguments

    outputType match {
      case OneToZeroOrOne =>
        val result = new Tuple()
        for (c <- projection.columns) {
          val value = c.evaluate(args)
          result.add(value)
        }
        functionCall.getOutputCollector().add(result)

      case OneToMany =>
        val entries = ArrayBuffer[IndexedSeq[Any]]()
        for (c <- projection.columns) {
          val value = c.evaluate(args)

          // note:  short-circuit (return) if any of the mappers returns a zero.
          c.operationType match {
            case PureMapper     => entries += IndexedSeq(value)
            case NullableMapper => if (value != null) entries += IndexedSeq(value) else return
            case OptionMapper   => if (value.asInstanceOf[Option[_]].isDefined) entries += IndexedSeq(value) else return
            case SeqMapper      => if (value.asInstanceOf[Seq[_]].nonEmpty) entries += value.asInstanceOf[Seq[_]].toIndexedSeq else return
          }
          val combinations = Combinations.combinations(entries)
          while (combinations.hasNext) {
            val combination = combinations.next
            val result = new Tuple()
            combination foreach { value => result.add(value) }
          }
        }

    }
  }

}

class MapOperation(val projection: Projection[_], val fields: Fields)
  extends BaseOperation[Any](projection.sourceFields.size, fields)
  with Function[Any]
{
  override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[Any]) {
    val result = new Tuple()
    val args = functionCall.getArguments
    for (c <- projection.columns) {
      val value = c.evaluate(args)
      Console.println("add %s -> %s" format (c, value))
      result.add(value)
    }
    functionCall.getOutputCollector().add(result)
  }
}

class MapSingleOperation(val expr: ColumnBase[_], val outputName: String)
  extends BaseOperation[Any](1, new Fields(outputName)) with Function[Any]
{
  override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[Any]) {
    val result = new Tuple()
    val args = functionCall.getArguments
    val value = expr.evaluate(args)
    result.add(value)
    functionCall.getOutputCollector().add(result)
  }
}
