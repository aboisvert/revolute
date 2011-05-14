package revolute.query

import revolute.util.NamingContext
import cascading.tuple.Fields

abstract class Table[T <: Product](_tableName: String) extends AbstractTable[T](_tableName) {
  def column[C: TypeMapper](n: String, options: ColumnOption[C]*) = new NamedColumn[C](this, n, options:_*)

  def innerJoin[U <: TableBase[_]](other: U) = new JoinBase[this.type, U](this, other, Join.Inner)
  def leftJoin[U <: TableBase[_]](other: U) = new JoinBase[this.type, U](this, other, Join.Left)
  def rightJoin[U <: TableBase[_]](other: U) = new JoinBase[this.type, U](this, other, Join.Right)
  def outerJoin[U <: TableBase[_]](other: U) = new JoinBase[this.type, U](this, other, Join.Outer)
}

abstract class BasicTable[T <: Product : TypeMapper](_tableName: String) extends AbstractBasicTable[T](_tableName) {
}

abstract class AbstractBasicTable[T <: Product](_tableName: String) extends AbstractTable[T](_tableName) {

  def column[C: TypeMapper](n: String, options: ColumnOption[C]*) = new NamedColumn[C](this, n, options:_*)

  def innerJoin[U <: TableBase[_]](other: U) = new JoinBase[this.type, U](this, other, Join.Inner)
  def leftJoin[U <: TableBase[_]](other: U) = new JoinBase[this.type, U](this, other, Join.Left)
  def rightJoin[U <: TableBase[_]](other: U) = new JoinBase[this.type, U](this, other, Join.Right)
  def outerJoin[U <: TableBase[_]](other: U) = new JoinBase[this.type, U](this, other, Join.Outer)
}

object BasicImplicitConversions {

  implicit def columnsToFields(p: Projection[_])(implicit context: NamingContext): Fields = p.fields

  implicit def baseColumnToColumnOps[B1 : BaseTypeMapper](c: Column[B1]): ColumnOps[B1, B1] = c match {
    case o: ColumnOps[_,_] => o.asInstanceOf[ColumnOps[B1, B1]]
    case _ => new ColumnOps[B1, B1] { protected[this] val leftOperand = c }
  }

  implicit def optionColumnToColumnOps[B1](c: Column[Option[B1]]): ColumnOps[B1, Option[B1]] = c match {
    case o: ColumnOps[_,_] => o.asInstanceOf[ColumnOps[B1, Option[B1]]]
    case _ => new ColumnOps[B1, Option[B1]] { protected[this] val leftOperand = c }
  }

  implicit def columnToOptionColumn[T : BaseTypeMapper](c: Column[T]): Column[Option[T]] = c.?

  implicit def valueToConstColumn[T : TypeMapper](v: T) = ConstColumn[T]("valueToConstColumn", v)

  implicit def tableToQuery[T <: ColumnBase[_]](t: T): Query[T] = new Query(t, Nil, Nil)

  implicit def columnToOrdering(c: Column[_]): Ordering = Ordering.Asc(By(c))
}