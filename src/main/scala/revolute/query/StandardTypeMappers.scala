package revolute.query

import revolute.QueryException

object StandardTypeMappers {
  object BooleanTypeMapperDelegate extends TypeMapperDelegate[Boolean] {
    def zero = false
  }

  object ByteTypeMapperDelegate extends TypeMapperDelegate[Byte] {
    def zero = 0
  }

  object ByteArrayTypeMapperDelegate extends TypeMapperDelegate[Array[Byte]] {
    val zero = new Array[Byte](0)
  }

  object DoubleTypeMapperDelegate extends TypeMapperDelegate[Double] {
    def zero = 0
  }

  object FloatTypeMapperDelegate extends TypeMapperDelegate[Float] {
    def zero = 0
  }

  object IntTypeMapperDelegate extends TypeMapperDelegate[Int] {
    def zero = 0
  }

  object LongTypeMapperDelegate extends TypeMapperDelegate[Long] {
    def zero = 0
  }

  object StringTypeMapperDelegate extends TypeMapperDelegate[String] {
    def zero = ""
  }

  object NullTypeMapperDelegate extends TypeMapperDelegate[AnyRef] {
    def zero = null
  }

  object UnitTypeMapperDelegate extends TypeMapperDelegate[Unit] {
    def zero = ()
  }
}