package revolute.util

object WithOp {
  def unapply(w: WithOp) = if (w.op == null) None else Some(w.op)
}

trait WithOp extends Cloneable { self: Node =>
  private var _op: Node = _

  def mapOp(f: Node => Node): this.type = {
    val t = clone
    t._op = f(this)
    t
  }

  final def op: Node = _op

  override def clone(): this.type = super.clone.asInstanceOf[this.type]
}

