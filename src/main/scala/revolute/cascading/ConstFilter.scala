package revolute.cascading

import cascading.flow.FlowProcess
import cascading.operation.{Filter, FilterCall, OperationCall}
import cascading.tuple.Fields

import revolute.query.{ColumnBase, Column, EvaluationContext}

import scala.collection._

/** A cascading filter against a constant value */
trait ConstFilter[T] extends Filter[T] with java.io.Serializable {

  def filter(x: T): Boolean

  override def isRemove(flowProcess: FlowProcess[_], filterCall: FilterCall[T]) = {
    filter(filterCall.getArguments.get(0).asInstanceOf[T])
  }
  override def isSafe = true
  override def getNumArgs = 1
  override def getFieldDeclaration = Fields.ALL
  override def prepare(flowProcess: FlowProcess[_], operationCall: OperationCall[T]) {}
  override def flush(flowProcess: FlowProcess[_], operationCall: OperationCall[T]) {}
  override def cleanup(flowProcess: FlowProcess[_], operationCall: OperationCall[T]) {}
}

/** Filter tuples whose field does not equal given constant value */
class EqualConstFilter[T](val value: T) extends ConstFilter[T] {
  override def filter(x: T) = !(x == value)
}

class LessThanConstFilter[T](val value: Comparable[T]) extends ConstFilter[T] {
  override def filter(x: T) = !(value.compareTo(x) < 0)
}

class LessOrEqualThanConstFilter[T](val value: Comparable[T]) extends ConstFilter[T] {
  override def filter(x: T) = !(value.compareTo(x) <= 0)
}

class GreaterThanConstFilter[T](val value: Comparable[T]) extends ConstFilter[T] {
  override def filter(x: T) = !(value.compareTo(x) > 0)
}

class GreaterOrEqualThanConstFilter[T](val value: Comparable[T]) extends ConstFilter[T] {
  override def filter(x: T) = !(value.compareTo(x) >= 0)
}

/** Filter tuples whose field value are not contained in given set of values */
class InSetFilter[T](val set: Set[T]) extends ConstFilter[T] {
  override def filter(x: T) = !(set contains x.asInstanceOf[T])
}

object IsFilter extends Filter[Any] with java.io.Serializable {
  def isRemove(flowProcess: FlowProcess[_], filterCall: FilterCall[Any]) = {
    filterCall.getArguments.get(0) != filterCall.getArguments.get(1)
  }
  def isSafe = true
  def getNumArgs = 2
  def getFieldDeclaration = Fields.ALL
  def prepare(flowProcess: FlowProcess[_], operationCall: OperationCall[Any]) {}
  def flush(flowProcess: FlowProcess[_], operationCall: OperationCall[Any]) {}
  def cleanup(flowProcess: FlowProcess[_], operationCall: OperationCall[Any]) {}
}

class ExpressionFilter(val column: Column[Boolean]) extends Filter[Any] with java.io.Serializable {
  @transient var chain: EvaluationChain = null

  override def isRemove(flowProcess: FlowProcess[_], filterCall: FilterCall[Any]) = {
    chain.tupleEntry = filterCall.getArguments
    val tuple = chain.context.nextTuple()
    if (tuple == null) true
    else ! (tuple.get(0).asInstanceOf[Boolean])
  }

  override def isSafe = true
  override def getNumArgs = EvaluationChain.inputFields(column).size
  override def getFieldDeclaration = Fields.ALL
  override def prepare(flowProcess: FlowProcess[_], operationCall: OperationCall[Any]) {
    chain = EvaluationChain.prepare(column, operationCall.getArgumentFields)
  }
  override def flush(flowProcess: FlowProcess[_], operationCall: OperationCall[Any]) {}
  override def cleanup(flowProcess: FlowProcess[_], operationCall: OperationCall[Any]) {}

  override def toString = "ExpressionFilter(%s)" format column
}
