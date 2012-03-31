package revolute.query

import cascading.tuple.{Fields, TupleEntry}
import revolute.query.TypeMapper._
import revolute.util.{Identifier, NamingContext, Tuple}
import revolute.util.Compat._
import scala.collection._
import scala.sys.error

abstract class Projection[T <: Product] extends ColumnBase[T] {
  type V = T

  import OperationType._
  import OutputType._

  // def <>[R](f: (T => R), g: (R => Option[T])): MappedProjection[R,T] = new MappedProjection(this, f, g)

  def projectionArity: Int

  def columns: Seq[ColumnBase[_]]

  override val nameHint = "Projection" + projectionArity

  override def dependencies = columns.toSet

  override val operationType = OperationType.PureMapper

  override def chainEvaluation(context: EvaluationContext): EvaluationContext = {
    new EvaluationContext {
      lazy val positions = columns map { c => c -> context.position(c) } toMap
      override def nextTuple(): Tuple = context.nextTuple()
      override def position(c: ColumnBase[_]) = positions(c)
      override def toString = "Projection.EvaluationContext(%s)" format context
    }
  }

  override def toString = "Projection%d(%s)" format (projectionArity, columns.toList)
}

object Projection {
  def unapply[T1,T2](p: Projection2[T1, T2]) = {
    val columns = p.unapply
    Some(columns._1, columns._2)
  }
  //def unapply[T1,T2](p: Projection2[T1,T2]) = Some(p)
  //def unapply[T1,T2,T3](p: Projection3[T1,T2,T3]) = Some(p)
  // def unapply[T1,T2,T3,T4](p: Projection4[T1,T2,T3,T4]) = Some(p)
  /*
  def unapply[T1,T2,T3,T4,T5](p: Projection5[T1,T2,T3,T4,T5]) = Some(p)
  def unapply[T1,T2,T3,T4,T5,T6](p: Projection6[T1,T2,T3,T4,T5,T6]) = Some(p)
  def unapply[T1,T2,T3,T4,T5,T6,T7](p: Projection7[T1,T2,T3,T4,T5,T6,T7]) = Some(p)
  def unapply[T1,T2,T3,T4,T5,T6,T7,T8](p: Projection8[T1,T2,T3,T4,T5,T6,T7,T8]) = Some(p)
  def unapply[T1,T2,T3,T4,T5,T6,T7,T8,T9](p: Projection9[T1,T2,T3,T4,T5,T6,T7,T8,T9]) = Some(p)
  def unapply[T1,T2,T3,T4,T5,T6,T7,T8,T9,T10](p: Projection10[T1,T2,T3,T4,T5,T6,T7,T8,T9,T10]) = Some(p)
  */
}

object ~ {
  def unapply[T1,T2](p: Projection2[T1,T2]) =
    Some(p._1, p._2)
  def unapply2[T1,T2,T3](p: Projection3[T1,T2,T3])(implicit t3: TypeMapper[T3]) =
    Some((new Projection2(p._1,p._2), p._3))
  /*
  def unapply[T1,T2,T3,T4](p: Projection4[T1,T2,T3,T4]) =
    Some((new Projection3(p._1,p._2,p._3), p._4))
  def unapply[T1,T2,T3,T4,T5](p: Projection5[T1,T2,T3,T4,T5]) =
    Some((new Projection4(p._1,p._2,p._3,p._4), p._5))
  def unapply[T1,T2,T3,T4,T5,T6](p: Projection6[T1,T2,T3,T4,T5,T6]) =
    Some((new Projection5(p._1,p._2,p._3,p._4,p._5), p._6))
  def unapply[T1,T2,T3,T4,T5,T6,T7](p: Projection7[T1,T2,T3,T4,T5,T6,T7]) =
    Some((new Projection6(p._1,p._2,p._3,p._4,p._5,p._6), p._7))
  def unapply[T1,T2,T3,T4,T5,T6,T7,T8](p: Projection8[T1,T2,T3,T4,T5,T6,T7,T8]) =
    Some((new Projection7(p._1,p._2,p._3,p._4,p._5,p._6,p._7), p._8))
  def unapply[T1,T2,T3,T4,T5,T6,T7,T8,T9](p: Projection9[T1,T2,T3,T4,T5,T6,T7,T8,T9]) =
    Some((new Projection8(p._1,p._2,p._3,p._4,p._5,p._6,p._7,p._8), p._9))
  def unapply[T1,T2,T3,T4,T5,T6,T7,T8,T9,T10](p: Projection10[T1,T2,T3,T4,T5,T6,T7,T8,T9,T10]) =
    Some((new Projection9(p._1,p._2,p._3,p._4,p._5,p._6,p._7,p._8,p._9), p._10))
  */
}

final class Projection1[T1](
  val _1: ColumnBase[T1]
) extends Projection[Tuple1[T1]] {
  override val projectionArity = 1
  def ~[U](c: ColumnBase[U]) = new Projection2(_1, c)
  def columns = Seq(_1)
}

final class Projection2[T1,T2](
  val _1: ColumnBase[T1],
  val _2: ColumnBase[T2]
) extends Projection[(T1,T2)] {
  override val projectionArity = 2
  def ~[U](c: ColumnBase[U]) = new Projection3(_1, _2, c)
  def columns: Seq[ColumnBase[_]] = Seq(_1, _2)
  def unapply = (_1, _2)

  /*
  def <>[R](f: ((T1,T2) => R), g: (R => Option[V])): MappedProjection[R,V] =
    <>(t => f(t._1,t._2), g)
  */
}

final class Projection3[T1,T2,T3](
  val _1: ColumnBase[T1],
  val _2: ColumnBase[T2],
  val _3: ColumnBase[T3]
) extends Projection[(T1,T2,T3)] {
  override val projectionArity = 3
  // def ~[U](c: Column[U]) = new Projection4(_1,_2,_3,c)
  def columns = Seq(_1, _2, _3)
  /*
  def <>[R](f: ((T1,T2,T3) => R), g: (R => Option[V])): MappedProjection[R,V] =
    <>(t => f(t._1,t._2,t._3), g)
  */
}

/*
final class Projection4[T1,T2,T3,T4](
  val _1: ColumnBase[T1],
  val _2: ColumnBase[T2],
  val _3: ColumnBase[T3],
  val _4: ColumnBase[T4]
)
extends Projection[(T1,T2,T3,T4)] {
  def columns = Seq(_1, _2, _3)
  // def ~[U](c: Column[U]) = new Projection5(_1,_2,_3,_4,c)
  /*
  override def mapOp(f: Node => Node): this.type = new Projection4(
    _1.mapOp(f),
    _2.mapOp(f),
    _3.mapOp(f),
    _4.mapOp(f)
  ).asInstanceOf[this.type]
  */
  /*
  def <>[R](f: ((T1,T2,T3,T4) => R), g: (R => Option[V])): MappedProjection[R,V] =
    <>(t => f(t._1,t._2,t._3,t._4), g)
    */
}

*/
/*

final class Projection5[T1,T2,T3,T4,T5](
  override val _1: ColumnBase[T1],
  override val _2: ColumnBase[T2],
  override val _3: ColumnBase[T3],
  override val _4: ColumnBase[T4],
  override val _5: ColumnBase[T5]
)
extends Tuple5(_1,_2,_3,_4,_5) with Projection[(T1,T2,T3,T4,T5)] {
  def ~[U](c: Column[U]) = new Projection6(_1,_2,_3,_4,_5,c)
  override def mapOp(f: Node => Node): this.type = new Projection5(
    _1.mapOp(f),
    _2.mapOp(f),
    _3.mapOp(f),
    _4.mapOp(f),
    _5.mapOp(f)
  ).asInstanceOf[this.type]
  def <>[R](f: ((T1,T2,T3,T4,T5) => R), g: (R => Option[V])): MappedProjection[R,V] =
    <>(t => f(t._1,t._2,t._3,t._4,t._5), g)
}

final class Projection6[T1,T2,T3,T4,T5,T6](
  override val _1: ColumnBase[T1],
  override val _2: ColumnBase[T2],
  override val _3: ColumnBase[T3],
  override val _4: ColumnBase[T4],
  override val _5: ColumnBase[T5],
  override val _6: ColumnBase[T6]
)
extends Tuple6(_1,_2,_3,_4,_5,_6) with Projection[(T1,T2,T3,T4,T5,T6)] {
  def ~[U](c: Column[U]) = new Projection7(_1,_2,_3,_4,_5,_6,c)
  override def mapOp(f: Node => Node): this.type = new Projection6(
    _1.mapOp(f),
    _2.mapOp(f),
    _3.mapOp(f),
    _4.mapOp(f),
    _5.mapOp(f),
    _6.mapOp(f)
  ).asInstanceOf[this.type]
  def <>[R](f: ((T1,T2,T3,T4,T5,T6) => R), g: (R => Option[V])): MappedProjection[R,V] =
    <>(t => f(t._1,t._2,t._3,t._4,t._5,t._6), g)
}

final class Projection7[T1,T2,T3,T4,T5,T6,T7](
  override val _1: ColumnBase[T1],
  override val _2: ColumnBase[T2],
  override val _3: ColumnBase[T3],
  override val _4: ColumnBase[T4],
  override val _5: ColumnBase[T5],
  override val _6: ColumnBase[T6],
  override val _7: ColumnBase[T7]
)
extends Tuple7(_1,_2,_3,_4,_5,_6,_7) with Projection[(T1,T2,T3,T4,T5,T6,T7)] {
  def ~[U](c: Column[U]) = new Projection8(_1,_2,_3,_4,_5,_6,_7,c)
  override def mapOp(f: Node => Node): this.type = new Projection7(
    _1.mapOp(f),
    _2.mapOp(f),
    _3.mapOp(f),
    _4.mapOp(f),
    _5.mapOp(f),
    _6.mapOp(f),
    _7.mapOp(f)
  ).asInstanceOf[this.type]
  def <>[R](f: ((T1,T2,T3,T4,T5,T6,T7) => R), g: (R => Option[V])): MappedProjection[R,V] =
    <>(t => f(t._1,t._2,t._3,t._4,t._5,t._6,t._7), g)
}

final class Projection8[T1,T2,T3,T4,T5,T6,T7,T8](
  override val _1: ColumnBase[T1],
  override val _2: ColumnBase[T2],
  override val _3: ColumnBase[T3],
  override val _4: ColumnBase[T4],
  override val _5: ColumnBase[T5],
  override val _6: ColumnBase[T6],
  override val _7: ColumnBase[T7],
  override val _8: ColumnBase[T8]
)
extends Tuple8(_1,_2,_3,_4,_5,_6,_7,_8) with Projection[(T1,T2,T3,T4,T5,T6,T7,T8)] {
  def ~[U](c: Column[U]) = new Projection9(_1,_2,_3,_4,_5,_6,_7,_8,c)
  override def mapOp(f: Node => Node): this.type = new Projection8(
    _1.mapOp(f),
    _2.mapOp(f),
    _3.mapOp(f),
    _4.mapOp(f),
    _5.mapOp(f),
    _6.mapOp(f),
    _7.mapOp(f),
    _8.mapOp(f)
  ).asInstanceOf[this.type]
  def <>[R](f: ((T1,T2,T3,T4,T5,T6,T7,T8) => R), g: (R => Option[V])): MappedProjection[R,V] =
    <>(t => f(t._1,t._2,t._3,t._4,t._5,t._6,t._7,t._8), g)
}

final class Projection9[T1,T2,T3,T4,T5,T6,T7,T8,T9](
  override val _1: ColumnBase[T1],
  override val _2: ColumnBase[T2],
  override val _3: ColumnBase[T3],
  override val _4: ColumnBase[T4],
  override val _5: ColumnBase[T5],
  override val _6: ColumnBase[T6],
  override val _7: ColumnBase[T7],
  override val _8: ColumnBase[T8],
  override val _9: ColumnBase[T9]
)
extends Tuple9(_1,_2,_3,_4,_5,_6,_7,_8,_9) with Projection[(T1,T2,T3,T4,T5,T6,T7,T8,T9)] {
  def ~[U](c: Column[U]) = new Projection10(_1,_2,_3,_4,_5,_6,_7,_8,_9,c)
  override def mapOp(f: Node => Node): this.type = new Projection9(
    _1.mapOp(f),
    _2.mapOp(f),
    _3.mapOp(f),
    _4.mapOp(f),
    _5.mapOp(f),
    _6.mapOp(f),
    _7.mapOp(f),
    _8.mapOp(f),
    _9.mapOp(f)
  ).asInstanceOf[this.type]
  def <>[R](f: ((T1,T2,T3,T4,T5,T6,T7,T8,T9) => R), g: (R => Option[V])): MappedProjection[R,V] =
    <>(t => f(t._1,t._2,t._3,t._4,t._5,t._6,t._7,t._8,t._9), g)
}

final class Projection10[T1,T2,T3,T4,T5,T6,T7,T8,T9,T10](
  override val _1: ColumnBase[T1],
  override val _2: ColumnBase[T2],
  override val _3: ColumnBase[T3],
  override val _4: ColumnBase[T4],
  override val _5: ColumnBase[T5],
  override val _6: ColumnBase[T6],
  override val _7: ColumnBase[T7],
  override val _8: ColumnBase[T8],
  override val _9: ColumnBase[T9],
  override val _10: ColumnBase[T10]
)
extends Tuple10(_1,_2,_3,_4,_5,_6,_7,_8,_9,_10) with Projection[(T1,T2,T3,T4,T5,T6,T7,T8,T9,T10)] {
  def ~[U](c: Column[U]) = error("no more projections dude!")
  override def mapOp(f: Node => Node): this.type = new Projection10(
    _1.mapOp(f),
    _2.mapOp(f),
    _3.mapOp(f),
    _4.mapOp(f),
    _5.mapOp(f),
    _6.mapOp(f),
    _7.mapOp(f),
    _8.mapOp(f),
    _9.mapOp(f),
    _10.mapOp(f)
  ).asInstanceOf[this.type]
  def <>[R](f: ((T1,T2,T3,T4,T5,T6,T7,T8,T9,T10) => R), g: (R => Option[V])): MappedProjection[R,V] =
    <>(t => f(t._1,t._2,t._3,t._4,t._5,t._6,t._7,t._8,t._9,t._10), g)
}

*/
