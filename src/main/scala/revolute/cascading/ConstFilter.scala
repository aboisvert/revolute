package revolute.cascading

import cascading.flow.FlowProcess
import cascading.operation.{Filter, FilterCall, OperationCall}
import cascading.tuple.Fields

/** A cascading filter against a constant value */
abstract class ConstFilter(val value: Any) extends Filter[Any] with java.io.Serializable {
  def filter(x: Any): Boolean
  def isRemove(flowProcess: FlowProcess, filterCall: FilterCall[Any]) = filter(filterCall.getArguments.get(0))
  def isSafe = true
  def getNumArgs = 1
  def getFieldDeclaration = Fields.ALL
  def prepare(flowProcess: FlowProcess, operationCall: OperationCall[Any]) = ()
  def cleanup(flowProcess: FlowProcess, operationCall: OperationCall[Any]) = ()
}

/** Tuple value must match constant, otherwise it is filtered */
class EqualFilter(_value: Any) extends ConstFilter(_value) {
  override def filter(x: Any) = (x != value)
}