package revolute.query

import revolute.util.Tuple
import revolute.cascading.SingleValueTuple

class Destructable2[T1, T2](c: ColumnBase[(T1, T2)]) {
  def columns(implicit t1: TypeMapper[T1], t2: TypeMapper[T2]): Projection2[T1, T2] = new Projection2(
    Destruction[T1](c, 0),
    Destruction[T2](c, 1)
  )
}

class Destructable3[T1, T2, T3](c: ColumnBase[(T1, T2, T3)]) {
  def columns(implicit t1: TypeMapper[T1], t2: TypeMapper[T2], t3: TypeMapper[T3]): Projection3[T1, T2, T3] = new Projection3(
    Destruction[T1](c, 0),
    Destruction[T2](c, 1),
    Destruction[T3](c, 2)
  )
}

case class Destruction[T](c: ColumnBase[_ <: Product], val index: Int) extends ColumnBase[T] {
  override val nameHint = c.nameHint + "(" + index + ")"
  override val operationType = OperationType.PureMapper
  override def dependencies = Set(c)
  override def chainEvaluation(parent: EvaluationContext) = new EvaluationContext {
    private[this] val tuple = new SingleValueTuple()
    override def nextTuple(): Tuple = {
      val next = parent.nextTuple()
      if (next == null) return null
      tuple.value = next.get(0).asInstanceOf[Product].productElement(index)
      tuple
    }
    override def position(c: ColumnBase[_]) = 0
  }
}
