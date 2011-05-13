package revolute.util

trait Expr[-V[_]] {
  def accept[T](vis: V[T]): T
}

/*
trait num[A] {
  def num(x: Int): A
}

case class Num[-V[X] <: num[X]](x: Int) extends Expr[V] {
  def accept[a](vis: V[a]): a = vis.num(x )
}

trait add[A] {
  def add(e1: A, e2: A): A
}

case class Add[-V[X] <: add[X]](e1: Expr[V], e2: Expr[V]) extends Expr[V] {
  def accept[a](vis: V[a]): a = vis.add(e1 .accept(vis), e2 .accept(vis))
}
*/