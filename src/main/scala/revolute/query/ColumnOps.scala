package revolute.query

import cascading.tuple.TupleEntry
import revolute.util.{Converter, Combinations}
import scala.collection._

abstract class ColumnOption[+T]

object ColumnOptions {
  case object NotNull extends ColumnOption[Nothing]
  case object Nullable extends ColumnOption[Nothing]
  case class Default[T](val defaultValue: T) extends ColumnOption[T]
}

trait SimpleFunction {
  val name: String
}

object SimpleFunction {
  /*
  def apply[T : TypeMapper](fname: String): (Seq[Column[_]] => OperatorColumn[T] with SimpleFunction) =
    (paramsC: Seq[Column[_]]) =>
      new OperatorColumn[T] with SimpleFunction {
        val name = fname
      }
  */
}

trait SimpleScalarFunction

/* B1 => Column's type, P1 is sometimes Option[B1] */
trait ColumnOps[B1, P1] {
  import ColumnOps._

  protected val leftOperand: Column[P1]

  def is(e: Column[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
    Is(leftOperand, e)

  def ===(e: Column[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
    Is(leftOperand, e)

  def isNot(e: Column[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
    Not(Is(leftOperand, e))

  def =!=(e: Column[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
    Not(Is(leftOperand, e))

  def <(e: Column[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
    LessThan(leftOperand, e)

  def <=(e: Column[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
   LessThanOrEqual(leftOperand, e)

  def >(e: Column[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
    GreaterThan(leftOperand, e)

  def >=(e: Column[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
    GreaterThanOrEqual(leftOperand, e)

  def in(set: Set[P1])(implicit o: scala.math.Ordering[P1]): OperatorColumn[Boolean] =
    InSet[P1](leftOperand, set)

  /*
  def between(start: Column[P1], end: Column[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
    Between[P1](leftOperand, start, end)
  */

  //def ifNull[B2, P2, R](e: Column[P2])(implicit om: OptionMapper2[B1, B2, Boolean, P1, P2, R]): Column[P2] =
  //  IfNull(leftOperand, e)
  /*
  def min(implicit om: OptionMapper2[B1, B1, B1, Option[B1], Option[B1], Option[B1]], tm: BaseTypeMapper[B1]): Column[Option[B1]] =
    om(Min(leftOperand, tm))
  def max(implicit om: OptionMapper2[B1, B1, B1, Option[B1], Option[B1], Option[B1]], tm: BaseTypeMapper[B1]): Column[Option[B1]] =
    om(Max(leftOperand, tm))
  */
  // NumericTypeMapper only
  /*
  def + [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, B1, P1, P2, R], tm: BaseTypeMapper[B1] with NumericTypeMapper): Column[R] =
    om(Arith[B1]("+", leftOperand, e))
  def - [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, B1, P1, P2, R], tm: BaseTypeMapper[B1] with NumericTypeMapper): Column[R] =
    om(Arith[B1]("-", leftOperand, e))
  def * [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, B1, P1, P2, R], tm: BaseTypeMapper[B1] with NumericTypeMapper): Column[R] =
    om(Arith[B1]("*", leftOperand, e))
  def / [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, B1, P1, P2, R], tm: BaseTypeMapper[B1] with NumericTypeMapper): Column[R] =
    om(Arith[B1]("/", leftOperand, e))
  def % [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, B1, P1, P2, R], tm: BaseTypeMapper[B1] with NumericTypeMapper): Column[R] =
    om(Mod[B1](leftOperand, e, tm))
  def abs(implicit om: OptionMapper2[B1, B1, B1, P1, P1, P1], tm: BaseTypeMapper[B1] with NumericTypeMapper): Column[P1] =
    om(Abs[B1](leftOperand, tm))
  def ceil(implicit om: OptionMapper2[B1, B1, B1, P1, P1, P1], tm: BaseTypeMapper[B1] with NumericTypeMapper): Column[P1] =
    om(Ceil[B1](leftOperand, tm))
  def floor(implicit om: OptionMapper2[B1, B1, B1, P1, P1, P1], tm: BaseTypeMapper[B1] with NumericTypeMapper): Column[P1] =
    om(Floor[B1](leftOperand, tm))
  def sign[R](implicit om: OptionMapper2[B1, B1, Int, P1, P1, R], tm: BaseTypeMapper[B1] with NumericTypeMapper): Column[R] =
    om(Sign(leftOperand))
  def avg(implicit om: OptionMapper2[B1, B1, B1, Option[B1], Option[B1], Option[B1]], tm: BaseTypeMapper[B1] with NumericTypeMapper): Column[Option[B1]] =
    om(Avg(leftOperand, tm))
  def sum(implicit om: OptionMapper2[B1, B1, B1, Option[B1], Option[B1], Option[B1]], tm: BaseTypeMapper[B1] with NumericTypeMapper): Column[Option[B1]] =
    om(Sum(leftOperand, tm))
  */

  def as[T2](implicit conv: Converter[P1, T2], tm: TypeMapper[T2]): OperatorColumn[T2]  = new As[P1, T2](leftOperand)

  def mapValue[T2](f: P1 => T2)(implicit tm: TypeMapper[T2]) = new MapValue[P1, T2](leftOperand, f)

  def mapOption[T2](f: P1 => Option[T2])(implicit tm: TypeMapper[T2]) = new MapOption[P1, T2](leftOperand, f)

  // Boolean only
  def &&(b: ColumnBase[P1])(implicit ev: P1 =:= Boolean): OperatorColumn[Boolean] =
    And(leftOperand.asInstanceOf[Column[Boolean]], b.asInstanceOf[Column[Boolean]])

  def ||(b: ColumnBase[P1])(implicit ev: P1 =:= Boolean): OperatorColumn[Boolean] =
    Or(leftOperand.asInstanceOf[Column[Boolean]], b.asInstanceOf[Column[Boolean]])

  def unary_!(implicit ev: P1 =:= Boolean): OperatorColumn[Boolean] =
    Not(leftOperand.asInstanceOf[Column[Boolean]])

  // String only
  def length(implicit ev: P1 =:= String): OperatorColumn[Int] =
    Length(leftOperand.asInstanceOf[Column[String]])

  def matches(e: String)(implicit ev: P1 =:= String): OperatorColumn[Boolean] =
    Regex(leftOperand.asInstanceOf[Column[String]], e)

  def ?(f: P1 => Boolean): OperatorColumn[Boolean] = Satisfies(leftOperand, f)

  def ++(e: Column[P1])(implicit ev: P1 =:= String): OperatorColumn[String] =
    Concat(leftOperand.asInstanceOf[Column[String]], e.asInstanceOf[Column[String]])

  /*
  def startsWith[R](s: String)(implicit om: OptionMapper2[String, String, Boolean, P1, P1, R]): Column[R] =
    om(new StartsWith(leftOperand, s))
  def endsWith[R](s: String)(implicit om: OptionMapper2[String, String, Boolean, P1, P1, R]): Column[R] =
    om(new EndsWith(leftOperand, s))
  */

  def toUpperCase(implicit ev: P1 =:= String): OperatorColumn[String] =
    ToUpperCase(leftOperand.asInstanceOf[Column[String]])

  def toLowerCase(implicit ev: P1 =:= String): OperatorColumn[String] =
    ToLowerCase(leftOperand.asInstanceOf[Column[String]])

  /*
  def ltrim[R](implicit om: OptionMapper2[String, String, String, P1, P1, R]): Column[R] =
    om(LTrim(leftOperand))
  def rtrim[R](implicit om: OptionMapper2[String, String, String, P1, P1, R]): Column[R] =
    om(RTrim(leftOperand))
  def trim[R](implicit om: OptionMapper2[String, String, String, P1, P1, R]): Column[R] =
    om(LTrim(RTrim(leftOperand)))
  */
}

sealed trait OperationType

object OperationType {
  case object PureMapper     extends OperationType // T1 => T2 (no filtering)
  case object NullableMapper extends OperationType // T1 => T2 and filters out null values
  case object OptionMapper   extends OperationType // T1 => Option[T2] and filters out `None` values
  case object SeqMapper      extends OperationType // T1 => Seq[T2]

  val values = List(PureMapper, NullableMapper, OptionMapper, SeqMapper)
}

object ColumnOps {
  import OperationType._

  // case class In(left: ColumnBase[_], right: ColumnBase[_]) extends OperatorColumn[Boolean] with ColumnOps[Boolean,Boolean]
  case class Count(query: ColumnBase[_]) extends ColumnBase[Int] {
    val name = "count"
    override def tables = query.tables
  }

  case class CountAll(query: ColumnBase[_]) extends ColumnBase[Int] {
    override def tables = query.tables
  }

  /*
  case class Mod[T](left: ColumnBase[_], right: ColumnBase[_], tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleScalarFunction {
    val name = "mod"
    override def tables = left.tables ++ right.tables
  }

  case class Abs[T](query: ColumnBase[_], tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleScalarFunction {
    val name = "abs"
    override def tables = query.tables
  }

  case class Ceil[T](query: ColumnBase[_], tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleScalarFunction {
    val name = "ceiling"
    override def tables = query.tables
  }

  case class Floor[T](query: ColumnBase[_], tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleScalarFunction {
    val name = "floor"
    override def tables = query.tables
  }

  case class Sign(query: ColumnBase[_]) extends OperatorColumn[Int] with SimpleScalarFunction {
    val name = "sign"
    override def tables = query.tables
  }

  case class Avg[T](query: ColumnBase[_], tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleFunction {
    val name = "avg"
    override def tables = query.tables
  }

  case class Min[T](query: ColumnBase[_], tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleFunction {
    val name = "min"
    override def tables = query.tables
  }

  case class Max[T](query: ColumnBase[_], tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleFunction {
    val name = "max"
    override def tables = query.tables
  }

  case class Sum[T](query: ColumnBase[_], tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleFunction {
    val name = "sum"
    override def tables = query.tables
  }
  */

  // L == Left
  // I == Intermediate Value, e.g. O, Option[O], Seq[O]
  // O == Output
  trait UnaryOperator[L, I, O] extends OperatorColumn[O] {
    val left: Column[L]

    override val leftOperand: Column[O] = this
    override def operationType: OperationType = PureMapper

    def apply(leftValue: L): I

    override final def evaluate(args: TupleEntry): Any = {
      left.operationType match {
        case PureMapper =>
          val leftResult = left.evaluate(args).asInstanceOf[L]
          apply(leftResult)

        case NullableMapper =>
          val leftResult = left.evaluate(args).asInstanceOf[L]
          if (leftResult == null) null
          else apply(leftResult)

        case OptionMapper =>
          val leftResult = left.evaluate(args).asInstanceOf[Option[L]]
          if (leftResult == null) null
          else leftResult match {
            case Some(value) => apply(value)
            case None        => null
          }

        case SeqMapper =>
          val leftResult = left.evaluate(args).asInstanceOf[Seq[L]]
          if (leftResult == null) null
          else operationType match {
            case PureMapper | NullableMapper | OptionMapper => leftResult map apply
            case SeqMapper =>
              val r = leftResult map apply
              if (r == null) null
              else r.asInstanceOf[Seq[Seq[I]]].filter(_ != null).flatten
          }
      }
    }

    final override def arguments = left.arguments
    final override def tables = left.tables
  }

  abstract class BinaryOperator[L, R, T, O: TypeMapper] extends OperatorColumn[O] {
    val left:  Column[L]
    val right: Column[R]

    override val leftOperand: Column[O] = this
    override def operationType: OperationType = PureMapper

    def apply(leftValue: L, rightValue: R): T

    override final def evaluate(args: TupleEntry): Any = {
      val leftResult = left.evaluate(args)

      // handle zeros left
      if      (left.operationType != PureMapper   && leftResult == null)      null
      else if (left.operationType == OptionMapper && leftResult == None)      null
      else if (left.operationType == SeqMapper    && leftResult == Seq.empty) null
      else {
        // no zeros on the left, evaluate right
        val rightResult = right.evaluate(args)

        // handle zeros on right
        if      (right.operationType != PureMapper   && rightResult == null)      null
        else if (right.operationType == OptionMapper && rightResult == None)      null
        else if (right.operationType == SeqMapper    && rightResult == Seq.empty) null

        // ok, we have to evaluate this expression
        else (left.operationType, right.operationType) match {
          case (PureMapper | NullableMapper, PureMapper | NullableMapper) =>
            apply(leftResult.asInstanceOf[L], rightResult.asInstanceOf[R])

          case (PureMapper | NullableMapper, OptionMapper) =>
            apply(leftResult.asInstanceOf[L], rightResult.asInstanceOf[Option[R]].get)

          case (PureMapper | NullableMapper, SeqMapper) =>
            val leftValue = leftResult.asInstanceOf[L]
            val result = rightResult.asInstanceOf[Seq[R]] map { rightValue => apply(leftValue, rightValue) }
            operationType match {
              case PureMapper | NullableMapper | OptionMapper => result
              case SeqMapper =>
                if (result == null) null
                else result.asInstanceOf[Seq[Seq[T]]].filter(_ != null).flatten
            }

          case (SeqMapper, PureMapper | NullableMapper) =>
            val rightValue = rightResult.asInstanceOf[R]
            val result = leftResult.asInstanceOf[Seq[L]] map { leftValue => apply(leftValue, rightValue) }
            operationType match {
              case PureMapper | NullableMapper | OptionMapper => result
              case SeqMapper =>
                if (result == null) null
                else result.asInstanceOf[Seq[Seq[T]]].filter(_ != null).flatten
            }

          case (OptionMapper, PureMapper | NullableMapper) =>
            apply(leftResult.asInstanceOf[Option[L]].get, rightResult.asInstanceOf[R])

          case (OptionMapper, OptionMapper) =>
            apply(leftResult.asInstanceOf[Option[L]].get, rightResult.asInstanceOf[Option[R]].get)

          case (OptionMapper, SeqMapper) =>
            val leftValue = leftResult.asInstanceOf[Option[L]].get
            val result = rightResult.asInstanceOf[Seq[R]] map { rightValue => apply(leftValue, rightValue) }
            operationType match {
              case PureMapper | NullableMapper | OptionMapper => result
              case SeqMapper =>
                if (result == null) null
                else result.asInstanceOf[Seq[Seq[T]]].filter(_ != null).flatten
            }

          case (SeqMapper, OptionMapper) =>
            val leftValue = leftResult.asInstanceOf[Option[L]].get
            val result = rightResult.asInstanceOf[Seq[R]] map { rightValue => apply(leftValue, rightValue) }
            operationType match {
              case PureMapper | NullableMapper | OptionMapper => result
              case SeqMapper =>
                if (result == null) null
                else result.asInstanceOf[Seq[Seq[T]]].filter(_ != null).flatten
            }

          case (SeqMapper, SeqMapper) =>
            val leftValues  = leftResult.asInstanceOf[Seq[L]].toIndexedSeq
            val rightValues = rightResult.asInstanceOf[Seq[R]].toIndexedSeq
            val combinations = Combinations.combinations(IndexedSeq(leftValues, rightValues)).toSeq
            combinations map { tuple => apply(tuple(0).asInstanceOf[L], tuple(1).asInstanceOf[R]) }
        } // else

      } // else-if
    } // evaluate

    final override def arguments = left.arguments ++ right.arguments
    final override def tables = left.tables ++ right.tables
  }

  case class As[T1, T2](val left: Column[T1])(implicit conv: Converter[T1, T2], tm: TypeMapper[T2])
    extends OperatorColumn[T2] with UnaryOperator[T1, T2, T2]
  {
    override def apply(leftValue: T1): T2 = conv(leftValue)
    override def toString = "As(%s, type=%s)" format (left.columnName getOrElse "N/A", tm)
  }

  case class MapValue[T1, T2](val left: Column[T1], f: T1 => T2)(implicit tm: TypeMapper[T2])
    extends OperatorColumn[T2] with UnaryOperator[T1, T2, T2]
  {
    override def apply(leftValue: T1): T2 = f(leftValue)
  }

  case class MapOption[T1, T2](val left: Column[T1], f: T1 => Option[T2])(implicit tm: TypeMapper[T2])
    extends OperatorColumn[T2] with UnaryOperator[T1, Option[T2], T2]
  {
    override def apply(leftValue: T1) = f(leftValue)
    override def operationType = OptionMapper
  }

  case class LessThan[O: scala.math.Ordering](val left: Column[O], val right: Column[O])
    extends BinaryOperator[O, O, Boolean, Boolean]
  {
    override def apply(leftValue: O, rightValue: O) = implicitly[Ordering[O]] lt (leftValue, rightValue)
  }

  case class LessThanOrEqual[O: scala.math.Ordering](val left: Column[O], val right: Column[O])
    extends BinaryOperator[O, O, Boolean, Boolean]
  {
    override def apply(leftValue: O, rightValue: O) = implicitly[Ordering[O]] lteq (leftValue, rightValue)
  }

  case class GreaterThan[O: scala.math.Ordering](val left: Column[O], val right: Column[O])
    extends BinaryOperator[O, O, Boolean, Boolean]
  {
    override def apply(leftValue: O, rightValue: O) = implicitly[Ordering[O]] gt (leftValue, rightValue)
  }

  case class GreaterThanOrEqual[O: scala.math.Ordering](val left: Column[O], val right: Column[O])
    extends BinaryOperator[O, O, Boolean, Boolean]
  {
    override def apply(leftValue: O, rightValue: O) = implicitly[Ordering[O]] gteq (leftValue, rightValue)
  }

  /*
  case class Exists(query: ColumnBase[_]) extends OperatorColumn[Boolean] with SimpleFunction with ColumnOps[Boolean,Boolean] {
    val name = "exists"
    override def tables = query.tables
  }
  */

  /*
  case class Arith[T : TypeMapper](name: String, left: ColumnBase[_], right: ColumnBase[_]) extends OperatorColumn[T] with SimpleBinaryOperator {
    override def tables = left.tables ++ right.tables
  }
  */

  /*
  case class IfNull(left: ColumnBase[_], right: ColumnBase[_]) extends SimpleScalarFunction {
    val name = "ifNull"
    override def tables = left.tables ++ right.tables
  }
  */

  case class Is[O: scala.math.Ordering](left: Column[O], right: Column[O])
    extends BinaryOperator[O, O, Boolean, Boolean] with ColumnOps[Boolean,Boolean]
  {
    override def apply(leftValue: O, rightValue: O) = implicitly[Ordering[O]] equiv (leftValue, rightValue)
  }

  case class CountDistinct(query: ColumnBase[_]) extends ColumnBase[Int] {
    override def tables = query.tables
  }

  case class InSet[T](override val left: Column[T], set: Set[T])
    extends UnaryOperator[T, Boolean, Boolean]
  {
    override def apply(leftValue: T): Boolean = set contains leftValue
    override def toString = "InSet(%s, %s)" format (left, set)
  }

  /*
  case class Between[O: Ordering](left: ColumnBase[O], start: ColumnBase[O], end: ColumnBase[O])
    extends OperatorColumn[Boolean] with UnaryOperator[O, Boolean, Boolean] with ColumnOps[Boolean, Boolean]
  {
    override def apply(leftValue: T1): T2 = {
      ((implicitly[Ordering[O]] gteq (leftValue, startValue)) && (implicitly[Ordering[O]] lteq (leftValue, endValue)))
    }

    }
    override def evaluate(args: TupleEntry): Boolean = {
      val leftValue = left.evaluate(args)
      val startValue = start.evaluate(args)
      val endValue = end.evaluate(args)
      ((implicitly[Ordering[O]] gteq (leftValue, startValue)) && (implicitly[Ordering[O]] lteq (leftValue, endValue)))
    }
  }
    */

  // Boolean
  case class And(val left: Column[Boolean], val right: Column[Boolean])
    extends BinaryOperator[Boolean, Boolean, Boolean, Boolean]
  {
    // TODO: would be nice to short-circuit if leftValue is false
    override def apply(leftValue: Boolean, rightValue: Boolean) = leftValue && rightValue
  }

  case class Or(left: Column[Boolean], right: Column[Boolean])
    extends BinaryOperator[Boolean, Boolean, Boolean, Boolean]
  {
    // TODO: would be nice to short-circuit if leftValue is true
    override def apply(leftValue: Boolean, rightValue: Boolean) = leftValue || rightValue
  }

  case class Not(left: Column[Boolean])
    extends UnaryOperator[Boolean, Boolean, Boolean]
  {
    override def apply(leftValue: Boolean): Boolean = !leftValue
  }

  // String
  case class Length(left: Column[String])
    extends UnaryOperator[String, Int, Int] with SimpleScalarFunction
  {
    val name = "length"
    override def apply(leftValue: String): Int = leftValue.length
  }

  case class ToUpperCase(left: Column[String])
    extends UnaryOperator[String, String, String] with SimpleScalarFunction
  {
    val name = "ucase"
    override def apply(leftValue: String): String = leftValue.toUpperCase
  }

  case class ToLowerCase(left: Column[String])
    extends UnaryOperator[String, String, String] with SimpleScalarFunction
  {
    val name = "lcase"
    override def apply(leftValue: String): String = leftValue.toLowerCase
  }

  /*
  case class LTrim(query: ColumnBase[_]) extends OperatorColumn[String] with SimpleScalarFunction {
    val name = "ltrim"
    override def tables = query.tables
  }

  case class RTrim(query: ColumnBase[_]) extends OperatorColumn[String] with SimpleScalarFunction {
    val name = "rtrim"
    override def tables = query.tables
  }
  */

  case class Regex(left: Column[String], s: String)
    extends OperatorColumn[Boolean] with UnaryOperator[String, Boolean, Boolean] with ColumnOps[Boolean,Boolean]
  {
    override def apply(leftValue: String) = leftValue.matches(s)
  }

  case class Satisfies[T](left: Column[T], f: T => Boolean)
    extends OperatorColumn[Boolean] with UnaryOperator[T, Boolean, Boolean] with ColumnOps[Boolean,Boolean]
  {
    override def apply(leftValue: T) = f(leftValue)
  }

  case class Concat(left: Column[String], right: Column[String])
    extends BinaryOperator[String, String, String, String] with SimpleScalarFunction
  {
    val name = "concat"
    override def apply(leftValue: String, rightValue: String) = leftValue + rightValue
  }

  /*
  class StartsWith(n: ColumnBase[_], s: String) extends Regex(n, ConstColumn(s, regexEscape(s) + "$"))
  class EndsWith(n: ColumnBase[_], s: String) extends Regex(n, ConstColumn(s, "^" + regexEscape(s)))
  */

  private def regexEscape(s: String) = """\Q""" + s + """\E"""
}
