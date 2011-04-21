package revolute.util

final class RefId[E <: AnyRef](val e: E) extends Function0[E] {
  def apply() = e
  override def hashCode = System.identityHashCode(e)
  override def equals(o: Any) = o match {
    case other: RefId[_] => e eq other.e
    case _ => false
  }
  override def toString = "RefId(" + e.toString + ")"
}
