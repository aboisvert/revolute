package revolute.cascading

import cascading.flow.FlowProcess
import cascading.operation.{BaseOperation, Function, FunctionCall, OperationCall}
import cascading.tuple.{Fields, Tuple}

import revolute.query.{ColumnBase, Column, OutputType, Projection}
import revolute.query.OperationType._
import revolute.util.{Combinations, NamingContext}

import scala.collection._
import scala.collection.mutable.ArrayBuffer

class FlatMapOperation(val projection: Projection[_])
  extends BaseOperation[Any](EvaluationChain.inputFields(projection).size, EvaluationChain.outputFields(projection)) with Function[Any]
{
  import OutputType._

  @transient var chain: EvaluationChain = null

  override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[Any]) {
    chain.tupleEntry = functionCall.getArguments
    var tuple = chain.context.nextTuple()
    Console.println("tuple: " + tuple)
    while (tuple != null) {
      try {
        val result = new cascading.tuple.Tuple()
        var i = 0
        while (i < chain.arity) {
          val value = tuple.get(i)
          result.add(value)
          i += 1
        }
        Console.println("add: " + result)
        functionCall.getOutputCollector().add(result)
      } catch { case EvaluationChain.NoValue => /* ignore tuple */ }
      tuple = chain.context.nextTuple()
    }
  }

  override def prepare(flowProcess: FlowProcess[_], operationCall: OperationCall[Any]) {
    chain = EvaluationChain.prepare(projection, operationCall.getArgumentFields)
  }
}
