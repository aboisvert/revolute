package revolute.query

import cascading.tuple.Fields
import revolute.util.NamingContext
import scala.collection.mutable.ArrayBuffer

abstract class Table extends AbstractTable[Product] {
  protected val _namedColumns = ArrayBuffer[NamedColumn[_]]()
  
  def column[C: TypeMapper](n: String, options: ColumnOption[C]*) = {
    val c = new NamedColumn[C](this, n, options:_*)
    _namedColumns += c
    c
  }
}
