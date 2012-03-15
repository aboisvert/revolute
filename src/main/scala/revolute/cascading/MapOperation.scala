package revolute.cascading

import cascading.flow.FlowProcess
import cascading.operation.{BaseOperation, Function, FunctionCall}
import cascading.tuple.{Fields, Tuple}

import revolute.query.{ColumnBase, Column, OutputType, Projection}
import revolute.query.OperationType._
import revolute.util.Combinations

import scala.collection._
import scala.collection.mutable.ArrayBuffer

class FlatMapOperation(val projection: Projection[_], val fields: Fields)
  extends BaseOperation[Any](projection.sourceFields.size, fields) with Function[Any]
{
  import OutputType._

  override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[Any]) {
    val args = functionCall.getArguments

    projection.outputType match {
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
