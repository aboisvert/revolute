package revolute.query

import revolute.cascading.EvaluationChain
import revolute.util.{NamingContext, StaticNamingContext}
import cascading.tuple.Fields

object ImplicitConversions {

  def inputFields(c: ColumnBase[_]): Fields = EvaluationChain.inputFields(c)

  def outputFields(c: ColumnBase[_]): Fields = EvaluationChain.outputFields(c)

  implicit def baseColumnToColumnOps[B1 : BaseTypeMapper](c: ColumnBase[B1]): ColumnOps[B1, B1] = c match {
    case o: ColumnOps[_,_] => o.asInstanceOf[ColumnOps[B1, B1]]
    case _ => new ColumnOps[B1, B1] { protected[this] val leftOperand = c }
  }

  implicit def optionColumnToColumnOps[B1](c: Column[Option[B1]]): ColumnOps[B1, Option[B1]] = c match {
    case o: ColumnOps[_,_] => o.asInstanceOf[ColumnOps[B1, Option[B1]]]
    case _ => new ColumnOps[B1, Option[B1]] { protected[this] val leftOperand = c }
  }

  implicit def columnToOptionColumn[T : BaseTypeMapper](c: Column[T]): Column[Option[T]] = c.?

  implicit def valueToConstColumn[T : TypeMapper](v: T) = ConstColumn[T]("valueToConstColumn", v)

  implicit def columnToQuery[T <: ColumnBase[_]](t: T): Query[T] = new Query(t, Nil, Nil)

  implicit def columnToResultOrdering(c: Column[_]): ResultOrdering = ResultOrdering.Asc(By(c))

  implicit def columnToDestructable2[T1, T2](c: ColumnBase[(T1, T2)]) = new Destructable2(c)
  implicit def columnToDestructable3[T1, T2, T3](c: ColumnBase[(T1, T2, T3)]) = new Destructable3(c)

  implicit def columnToProjection[T](c: ColumnBase[T]): Projection1[T] = new Projection1[T](c)

  implicit def queryToProjection[P <: Projection[_]](q: Query[P]) = q.value
}