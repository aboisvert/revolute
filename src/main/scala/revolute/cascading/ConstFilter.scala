package revolute.cascading

import cascading.flow.FlowProcess
import cascading.operation.{Filter, FilterCall, OperationCall}
import cascading.tuple.Fields

/** A cascading filter against a constant value */
trait ConstFilter[T] extends Filter[T] with java.io.Serializable {
  def filter(x: Any): Boolean
  def isRemove(flowProcess: FlowProcess, filterCall: FilterCall[T]) = filter(filterCall.getArguments.get(0))
  def isSafe = true
  def getNumArgs = 1
  def getFieldDeclaration = Fields.ALL
  def prepare(flowProcess: FlowProcess, operationCall: OperationCall[T]) = ()
  def cleanup(flowProcess: FlowProcess, operationCall: OperationCall[T]) = ()
}

/** Filter tuples whose field does not equal given constant value */
class EqualConstFilter[T](val value: T) extends ConstFilter[T] {
  override def filter(x: Any) = (x != value)
}

/** Filter tuples whose field value are not contained in given set of values */
class InSetFilter[T](val set: Set[T]) extends ConstFilter[T] {
  override def filter(x: Any) = {
    Console.println("InSet: " + set)
    !(set contains x.asInstanceOf[T])
  }
}

object IsFilter extends Filter[Any] with java.io.Serializable {
  def isRemove(flowProcess: FlowProcess, filterCall: FilterCall[Any]) = {
    filterCall.getArguments.get(0) != filterCall.getArguments.get(1)
  }
  def isSafe = true
  def getNumArgs = 2
  def getFieldDeclaration = Fields.ALL
  def prepare(flowProcess: FlowProcess, operationCall: OperationCall[Any]) = ()
  def cleanup(flowProcess: FlowProcess, operationCall: OperationCall[Any]) = ()
}
