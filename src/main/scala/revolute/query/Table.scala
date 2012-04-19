package revolute.query

import revolute.util.NamingContext
import cascading.tuple.Fields

abstract class Table[T <: Product](_tableName: String) extends AbstractTable[T](_tableName) {
  def column[C: TypeMapper](n: String, options: ColumnOption[C]*) = new NamedColumn[C](this, n, options:_*)
}
