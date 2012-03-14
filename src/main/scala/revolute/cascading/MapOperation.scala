package revolute.cascading

import cascading.flow.FlowProcess
import cascading.operation.{BaseOperation, Function, FunctionCall}
import cascading.tuple.{Fields, Tuple}

import revolute.query.{ColumnBase, Column, Projection}
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
  extends BaseOperation[Any](projection.sourceFields.size, fields) with Function[Any]
{
  import OutputType._

  lazy val outputType = {
    val operationTypes = projection.columns map (_.operationType)
    if (operationTypes exists (_ == SeqMapper)) OneToMany else OneToZeroOrOne
  }

  override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[Any]) {
    val args = functionCall.getArguments

    outputType match {
      case OneToZeroOrOne =>
        val result = new Tuple()
        for (c <- projection.columns) {
          val value = c.evaluate(args)
          c.operationType match {
            case PureMapper     => result.add(value)
            case NullableMapper => if (value != null) result.add(value) else return
            case OptionMapper   => value match { case Some(value) => result.add(value); case None => return }
            case unexpected     => sys.error("Unexpected operationType: " + unexpected)
          }
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
            case OptionMapper   => value match { case Some(value) => entries += IndexedSeq(value); case None => return }
            case SeqMapper      => value match { case seq: Seq[_] if seq.nonEmpty => entries += seq.toIndexedSeq; case _ => return }
          }
        }
        val combinations = Combinations.combinations(entries)
        while (combinations.hasNext) {
          val combination = combinations.next
          val result = new Tuple()
          combination foreach { value => result.add(value) }
          functionCall.getOutputCollector().add(result)
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
      result.add(value)
    }
    functionCall.getOutputCollector().add(result)
  }
}

class MapSingleOperation(val expr: Column[_], val outputName: String)
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
