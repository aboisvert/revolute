package revolute.cascading

import cascading.flow.FlowProcess
import cascading.operation.{BaseOperation, Function, FunctionCall}
import cascading.tuple.{Fields, Tuple}

class ConcatOperation() extends BaseOperation[String](2, new Fields("concat")) with Function[String] {

  override def operate(flowProcess: FlowProcess, functionCall: FunctionCall[String]) {
    val args = functionCall.getArguments();
    val result = new Tuple()
    val concat = args.getString(0) + args.getString(1)
    result.add(concat)
    functionCall.getOutputCollector().add(result)
  }

}
