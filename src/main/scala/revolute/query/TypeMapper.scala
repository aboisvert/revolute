package revolute.query

import revolute.QueryException
import scala.collection._

/**
 * A (usually implicit) TypeMapper object represents a Scala type that can be
 * used as a column type.
 */
trait TypeMapper[T] extends java.io.Serializable { self =>
  def apply: TypeMapperDelegate[T]

  def createOptionTypeMapper: OptionTypeMapper[T] = new OptionTypeMapper[T](self) {
    def apply = self.apply.createOptionTypeMapperDelegate
    def getBaseTypeMapper[U](implicit ev: Option[U] =:= Option[T]): TypeMapper[U] = self.asInstanceOf[TypeMapper[U]]
  }

  def getBaseTypeMapper[U](implicit ev: Option[U] =:= T): TypeMapper[U]
}

object TypeMapper {
  implicit def typeMapperToOptionTypeMapper[T](implicit t: TypeMapper[T]): OptionTypeMapper[T] = t.createOptionTypeMapper

  implicit object BooleanTypeMapper extends BaseTypeMapper[Boolean] {
    def apply = StandardTypeMappers.BooleanTypeMapperDelegate
  }

  implicit object ByteTypeMapper extends BaseTypeMapper[Byte] with NumericTypeMapper {
    def apply = StandardTypeMappers.ByteTypeMapperDelegate
  }

  implicit object ByteArrayTypeMapper extends BaseTypeMapper[Array[Byte]] {
    def apply = StandardTypeMappers.ByteArrayTypeMapperDelegate
  }

  implicit object DoubleTypeMapper extends BaseTypeMapper[Double] with NumericTypeMapper {
    def apply = StandardTypeMappers.DoubleTypeMapperDelegate
  }

  implicit object FloatTypeMapper extends BaseTypeMapper[Float] with NumericTypeMapper {
    def apply = StandardTypeMappers.FloatTypeMapperDelegate
  }

  implicit object IntTypeMapper extends BaseTypeMapper[Int] with NumericTypeMapper {
    def apply = StandardTypeMappers.IntTypeMapperDelegate
  }

  implicit object LongTypeMapper extends BaseTypeMapper[Long] with NumericTypeMapper {
    def apply = StandardTypeMappers.LongTypeMapperDelegate
  }

  implicit object StringTypeMapper extends BaseTypeMapper[String] {
    def apply = StandardTypeMappers.StringTypeMapperDelegate
  }

  implicit object NullTypeMapper extends BaseTypeMapper[AnyRef] {
    def apply = StandardTypeMappers.NullTypeMapperDelegate
  }

  implicit object UnitTypeMapper extends BaseTypeMapper[Unit] {
    def apply = StandardTypeMappers.UnitTypeMapperDelegate
  }

  implicit def mapTypeMapper[K, V](implicit k: TypeMapper[K], v: TypeMapper[V]): TypeMapper[Map[K, V]] = new MapTypeMapper[K, V]

  implicit def tuple1TypeMapper[T1](implicit t1: TypeMapper[T1]) = new BaseTypeMapper[Tuple1[T1]] {
    def apply = new TypeMapperDelegate[Tuple1[T1]] {
      override val zero = Tuple1(t1.apply.zero)
    }
  }

  implicit def tuple2TypeMapper[T1, T2](implicit t1: TypeMapper[T1], t2: TypeMapper[T2]) = new BaseTypeMapper[(T1, T2)] {
    def apply = new TypeMapperDelegate[(T1, T2)] {
      override val zero = (t1.apply.zero, t2.apply.zero)
    }
  }

  implicit def tuple3TypeMapper[T1, T2, T3](implicit t12: TypeMapper[(T1, T2)], t3: TypeMapper[T3]) = new BaseTypeMapper[(T1, T2, T3)] {
    def apply = new TypeMapperDelegate[(T1, T2, T3)] {
      override val zero = {
        val (zero1, zero2) = t12.apply.zero
        (zero1, zero2, t3.apply.zero)
      }
    }
  }
}

trait BaseTypeMapper[T] extends TypeMapper[T] {
  def getBaseTypeMapper[U](implicit ev: Option[U] =:= T) =
    throw new QueryException("A BaseTypeMapper should not have an Option type")

  override def toString = getClass.getSimpleName
}

abstract class OptionTypeMapper[T](val base: TypeMapper[T]) extends TypeMapper[Option[T]]

class MapTypeMapper[K, V] extends BaseTypeMapper[Map[K, V]] {
  def apply = new TypeMapperDelegate[Map[K, V]] {
    val zero = mutable.Map[K, V]()
  }
}

/**
 * Adding this marker trait to a TypeMapper makes the type eligible for
 * numeric operators.
 */
trait NumericTypeMapper

trait TypeMapperDelegate[T] { self =>
  /**
   * A zero value for the type. This is used as a default instead of NULL when
   * used as a non-nullable column.
   */
  def zero: T
  def nullable = false

  def createOptionTypeMapperDelegate: TypeMapperDelegate[Option[T]] = new TypeMapperDelegate[Option[T]] {
    def zero = None
    override def nullable = true
  }
}

abstract class MappedTypeMapper[T,U](implicit tm: TypeMapper[U]) extends TypeMapper[T] { self =>
  def map(t: T): U
  def comap(u: U): T

  def nullable: Option[Boolean] = None

  def apply: TypeMapperDelegate[T] = new TypeMapperDelegate[T] {
    val tmd = tm.apply
    def zero = comap(tmd.zero)
    override def nullable = self.nullable.getOrElse(tmd.nullable)
  }
}

object MappedTypeMapper {
  def base[T, U](tmap: T => U, tcomap: U => T)(implicit tm: TypeMapper[U]): BaseTypeMapper[T] =
    new MappedTypeMapper[T, U] with BaseTypeMapper[T] {
      def map(t: T) = tmap(t)
      def comap(u: U) = tcomap(u)
    }
}
