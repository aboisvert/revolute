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
