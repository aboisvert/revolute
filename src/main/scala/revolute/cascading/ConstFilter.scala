package revolute.cascading

import cascading.flow.FlowProcess
import cascading.operation.{Filter, FilterCall, OperationCall}
import cascading.tuple.Fields

import revolute.query.ColumnBase

import scala.collection._

/** A cascading filter against a constant value */
trait ConstFilter[T] extends Filter[T] with java.io.Serializable {
  def filter(x: T): Boolean
  def isRemove(flowProcess: FlowProcess, filterCall: FilterCall[T]) = filter(filterCall.getArguments.get(0).asInstanceOf[T])
  def isSafe = true
  def getNumArgs = 1
  def getFieldDeclaration = Fields.ALL
  def prepare(flowProcess: FlowProcess, operationCall: OperationCall[T]) {}
  def cleanup(flowProcess: FlowProcess, operationCall: OperationCall[T]) {}
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
  def isRemove(flowProcess: FlowProcess, filterCall: FilterCall[Any]) = {
    filterCall.getArguments.get(0) != filterCall.getArguments.get(1)
  }
  def isSafe = true
  def getNumArgs = 2
  def getFieldDeclaration = Fields.ALL
  def prepare(flowProcess: FlowProcess, operationCall: OperationCall[Any]) {}
  def cleanup(flowProcess: FlowProcess, operationCall: OperationCall[Any]) {}
}

object WithArguments {
}

class ExpressionFilter(val expr: ColumnBase[Boolean]) extends Filter[Any] with java.io.Serializable {
  def isRemove(flowProcess: FlowProcess, filterCall: FilterCall[Any]) = {
    !expr.evaluate(arguments(filterCall))
  }

  private def arguments(filterCall: FilterCall[_]): Map[String, Any] = {
    val map = mutable.Map.empty[String, Any]
    expr.arguments.zipWithIndex foreach { case (name, i) =>
      val pos = filterCall.getArgumentFields.getPos(name)
      map(name) = filterCall.getArguments.get(pos)
    }
    map
  }

  def isSafe = true
  def getNumArgs = expr.arity
  def getFieldDeclaration = Fields.ALL
  def prepare(flowProcess: FlowProcess, operationCall: OperationCall[Any]) {}
  def cleanup(flowProcess: FlowProcess, operationCall: OperationCall[Any]) {}
}
