package revolute.util

object Compat {
  // type alias to deal with Java wildcards
  type Tap = cascading.tap.Tap[_ <: cascading.flow.FlowProcess[_], _, _, _]

  implicit def arrayToComparable[T <: Comparable[_]](a: Array[T]) = a.asInstanceOf[Array[Comparable[_]]]
}