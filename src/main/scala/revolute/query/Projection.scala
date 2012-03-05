package revolute.query

import cascading.tuple.Fields
import revolute.util.NamingContext
import scala.collection._
import scala.sys.error

sealed trait Projection[T <: Product] extends ColumnBase[T] with Product {
  type V = T

  // def <>[R](f: (T => R), g: (R => Option[T])): MappedProjection[R,T] = new MappedProjection(this, f, g)

  def columns = productIterator map (_.asInstanceOf[Column[_]])

  def fields(implicit context: NamingContext): Fields = {
    val names: Array[Comparable[_]] = columns map { c => c.columnName getOrElse (context.nameFor(c)) } toArray
    val fields: Fields = new Fields(names: _*)
    Console.println("projection {%s} fields %s " format (columns.toList, fields))
    fields
  }

  def sourceFields: Fields = {
    val names: Array[Comparable[_]] = columns flatMap (_.arguments.iterator) toArray
    val fields: Fields = new Fields(names: _*)
    Console.println("projection {%s} source fields %s " format (columns.toList, fields))
    fields
  }

  override def arguments = (columns.toSeq map (_.arguments) flatten) toSet

  override def evaluate(args: Map[String, Any]): T = error("todo")

  override def tables = {
    Set() ++ columns flatMap (_.tables)
  }

  override def toString = "Projection%d(%s)" format (productArity, productIterator.toList)
}

object Projection {
  def unapply[T1,T2](p: Projection2[T1,T2]) = Some(p)
  def unapply[T1,T2,T3](p: Projection3[T1,T2,T3]) = Some(p)
  def unapply[T1,T2,T3,T4](p: Projection4[T1,T2,T3,T4]) = Some(p)
  /*
  def unapply[T1,T2,T3,T4,T5](p: Projection5[T1,T2,T3,T4,T5]) = Some(p)
  def unapply[T1,T2,T3,T4,T5,T6](p: Projection6[T1,T2,T3,T4,T5,T6]) = Some(p)
  def unapply[T1,T2,T3,T4,T5,T6,T7](p: Projection7[T1,T2,T3,T4,T5,T6,T7]) = Some(p)
  def unapply[T1,T2,T3,T4,T5,T6,T7,T8](p: Projection8[T1,T2,T3,T4,T5,T6,T7,T8]) = Some(p)
  def unapply[T1,T2,T3,T4,T5,T6,T7,T8,T9](p: Projection9[T1,T2,T3,T4,T5,T6,T7,T8,T9]) = Some(p)
  def unapply[T1,T2,T3,T4,T5,T6,T7,T8,T9,T10](p: Projection10[T1,T2,T3,T4,T5,T6,T7,T8,T9,T10]) = Some(p)
  */
}

/*
class MappedProjection[T, P <: Product](val child: Projection[P], f: (P => T), g: (T => Option[P])) extends ColumnBase[T] {
  override def toString = "MappedProjection"
  override def arguments = child.arguments
  override def evaluate(args: Map[String, Any]): T = error("todo")
  override def tables = child.tables
}
*/

object ~ {
  def unapply[T1,T2](p: Projection2[T1,T2]) =
    Some(p)
  def unapply[T1,T2,T3](p: Projection3[T1,T2,T3]) =
    Some((new Projection2(p._1,p._2), p._3))
  def unapply[T1,T2,T3,T4](p: Projection4[T1,T2,T3,T4]) =
    Some((new Projection3(p._1,p._2,p._3), p._4))
  /*
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

final class Projection2[T1,T2](
  override val _1: Column[T1],
  override val _2: Column[T2]
) extends Tuple2(_1,_2) with Projection[(T1,T2)] {
  def ~[U](c: Column[U]) = new Projection3(_1,_2,c)
  /*
  def <>[R](f: ((T1,T2) => R), g: (R => Option[V])): MappedProjection[R,V] =
    <>(t => f(t._1,t._2), g)
  */
}

final class Projection3[T1,T2,T3](
  override val _1: Column[T1],
  override val _2: Column[T2],
  override val _3: Column[T3]
)
extends Tuple3(_1,_2,_3) with Projection[(T1,T2,T3)] {
  def ~[U](c: Column[U]) = new Projection4(_1,_2,_3,c)
  /*
  def <>[R](f: ((T1,T2,T3) => R), g: (R => Option[V])): MappedProjection[R,V] =
    <>(t => f(t._1,t._2,t._3), g)
  */
}

final class Projection4[T1,T2,T3,T4](
  override val _1: Column[T1],
  override val _2: Column[T2],
  override val _3: Column[T3],
  override val _4: Column[T4]
)
extends Tuple4(_1,_2,_3,_4) with Projection[(T1,T2,T3,T4)] {
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

/*

final class Projection5[T1,T2,T3,T4,T5](
  override val _1: Column[T1],
  override val _2: Column[T2],
  override val _3: Column[T3],
  override val _4: Column[T4],
  override val _5: Column[T5]
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
  override val _1: Column[T1],
  override val _2: Column[T2],
  override val _3: Column[T3],
  override val _4: Column[T4],
  override val _5: Column[T5],
  override val _6: Column[T6]
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
  override val _1: Column[T1],
  override val _2: Column[T2],
  override val _3: Column[T3],
  override val _4: Column[T4],
  override val _5: Column[T5],
  override val _6: Column[T6],
  override val _7: Column[T7]
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
  override val _1: Column[T1],
  override val _2: Column[T2],
  override val _3: Column[T3],
  override val _4: Column[T4],
  override val _5: Column[T5],
  override val _6: Column[T6],
  override val _7: Column[T7],
  override val _8: Column[T8]
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
  override val _1: Column[T1],
  override val _2: Column[T2],
  override val _3: Column[T3],
  override val _4: Column[T4],
  override val _5: Column[T5],
  override val _6: Column[T6],
  override val _7: Column[T7],
  override val _8: Column[T8],
  override val _9: Column[T9]
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
  override val _1: Column[T1],
  override val _2: Column[T2],
  override val _3: Column[T3],
  override val _4: Column[T4],
  override val _5: Column[T5],
  override val _6: Column[T6],
  override val _7: Column[T7],
  override val _8: Column[T8],
  override val _9: Column[T9],
  override val _10: Column[T10]
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
