package revolute.cascading

import cascading.flow.FlowProcess
import cascading.operation.{BaseOperation, Function, FunctionCall}
import cascading.tuple.{Fields, Tuple}

import revolute.query.{ColumnBase, Projection}

import scala.collection._

class MapOperation(val projection: Projection[_], val fields: Fields) extends BaseOperation[Any](projection.columns.size, fields) with Function[Any] {

  override def operate(flowProcess: FlowProcess, functionCall: FunctionCall[Any]) {
    val result = new Tuple()
    val args = arguments(functionCall)
    for (c <- projection.columns) {
      val value = c.evaluate(args)
      result.add(value)
    }
    functionCall.getOutputCollector().add(result)
  }

  private def arguments(functionCall: FunctionCall[_]): Map[String, Any] = {
    val map = mutable.Map.empty[String, Any]
    projection.arguments.zipWithIndex foreach { case (name, i) =>
      val pos = functionCall.getArgumentFields.getPos(name)
      map(name) = functionCall.getArguments.get(pos)
    }
    map
  }
}

class MapSingleOperation(val expr: ColumnBase[_], val outputName: String)
  extends BaseOperation[Any](1, new Fields(outputName)) with Function[Any]
{

  override def operate(flowProcess: FlowProcess, functionCall: FunctionCall[Any]) {
    val result = new Tuple()
    val args = arguments(functionCall)
    val value = expr.evaluate(args)
    result.add(value)
    functionCall.getOutputCollector().add(result)
  }

  private def arguments(functionCall: FunctionCall[_]): Map[String, Any] = {
    val map = mutable.Map.empty[String, Any]
    expr.arguments.zipWithIndex foreach { case (name, i) =>
      val pos = functionCall.getArgumentFields.getPos(name)
      map(name) = functionCall.getArguments.get(pos)
    }
    map
  }
}
