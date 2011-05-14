package revolute.cascading

import cascading.flow.FlowProcess
import cascading.operation.{BaseOperation, Function, FunctionCall}
import cascading.tuple.{Fields, Tuple}

class ConcatOperation() extends BaseOperation[String](2, new Fields("concat")) with Function[String] {

  // Why? because getString(Comparable) troubles the compiler when passing int's
  private val zero = new java.lang.Integer(0)
  private val one = new java.lang.Integer(1)

  override def operate(flowProcess: FlowProcess, functionCall: FunctionCall[String]) {
    val args = functionCall.getArguments();
    val result = new Tuple()
    val concat = args.getString(zero) + args.getString(one)
    result.add(concat)
    functionCall.getOutputCollector().add(result)
  }

}
