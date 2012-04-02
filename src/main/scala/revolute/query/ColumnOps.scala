package revolute.query

import cascading.tuple.TupleEntry
import revolute.util.{Converter, Combinations, Identifier, Tuple}
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

  def asColumnOf[T2](implicit conv: Converter[P1, T2], tm: TypeMapper[T2]): OperatorColumn[T2]  = new AsColumnOf[P1, T2](leftOperand)

  def mapValue[T2](f: P1 => T2)(implicit tm: TypeMapper[T2]) = new MapValue[P1, T2](leftOperand, f)

  def mapOption[T2](f: P1 => Option[T2])(implicit tm: TypeMapper[T2]) = new MapOption[P1, T2](leftOperand, f)

  def mapPartial[T2](f: PartialFunction[P1,T2])(implicit tm: TypeMapper[T2]) = new MapPartial[P1, T2](leftOperand, f)

  def mapSeq[T2](f: P1 => Seq[T2])(implicit tm: TypeMapper[T2]) = new MapSeq[P1, T2](leftOperand, f)

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
  case class Count(query: ColumnBase[_]) extends SyntheticColumn[Int] {
  }

  case class CountAll(query: ColumnBase[_]) extends SyntheticColumn[Int] {
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

    def apply(leftValue: L): I

    override def chainEvaluation(parent: EvaluationContext): EvaluationContext = new EvaluationContext {
      lazy val pos = parent.position(left)

      private[this] var value: I = _

      private[this] val tuple = new Tuple {
        override def get(pos: Int) = value
      }

      override def nextTuple(): Tuple = {
        Console.println("unary next tuple %s" format nameHint)
        val nextTuple = parent.nextTuple()
        Console.println("unary next tuple %s: %s" format (nameHint, nextTuple))
        if (nextTuple == null) return null
        value = apply(nextTuple.get(pos).asInstanceOf[L])
        Console.println("unary next tuple2 %s: %s" format (nameHint, tuple))
        tuple
      }

      override def position(c: ColumnBase[_]) = 0

      override def toString = "%s.EvaluationContext(parent=%s)" format (UnaryOperator.this.toString, parent)
    }

    final override def dependencies = Set(left)
  }

  abstract class BinaryOperator[L, R, T, O: TypeMapper] extends OperatorColumn[O] {
    val left:  Column[L]
    val right: Column[R]

    override val leftOperand: Column[O] = this

    def apply(leftValue: L, rightValue: R): T

    override def chainEvaluation(parent: EvaluationContext): EvaluationContext = new EvaluationContext {
      lazy val leftPos = parent.position(left)
      lazy val rightPos = parent.position(right)

      private[this] var value: T = null.asInstanceOf[T]

      private[this] val tuple = new Tuple {
        override def get(pos: Int) = value
        override def toString = "BinaryOperator.Tuple(%s)" format value
      }

      override def nextTuple(): Tuple = {
        Console.println("binary next tuple %s" format nameHint)
        val nextTuple = parent.nextTuple()
        if (nextTuple == null) return null
        val left = nextTuple.get(leftPos).asInstanceOf[L]
        val right = nextTuple.get(rightPos).asInstanceOf[R]
        Console.println("binary eval %s: left=%s right=%s" format (nameHint, left, right))
        value = apply(left, right)
        Console.println("binary next tuple %s: %s" format (nameHint, tuple))
        tuple
      }

      override def position(c: ColumnBase[_]) = 0

      override def toString = "%s.EvaluationContext(parent=%s)" format (BinaryOperator.this.toString, parent)
    }

    final override def dependencies = Set(left, right)
  }

  case class AsColumnOf[T1, T2](val left: Column[T1])(implicit conv: Converter[T1, T2], tm: TypeMapper[T2])
    extends OperatorColumn[T2] with UnaryOperator[T1, T2, T2]
  {
    override val nameHint = "AsColumnOf[%s](%s)" format (tm.getClass.getSimpleName, left.nameHint)
    override def apply(leftValue: T1): T2 = conv(leftValue)
    override def toString = "AsColumnOf(%s, type=%s)" format (left, tm.getClass.getSimpleName)
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
    override def operationType = OperationType.OptionMapper
  }

  case class MapPartial[T1, T2](val left: Column[T1], f: PartialFunction[T1, T2])(implicit tm: TypeMapper[T2])
    extends OperatorColumn[T2] with UnaryOperator[T1, Option[T2], T2]
  {
    override def apply(leftValue: T1) = if (f.isDefinedAt(leftValue)) Some(f(leftValue)) else None
    override def operationType = OperationType.OptionMapper
  }

  case class MapSeq[T1, T2](val left: Column[T1], f: T1 => Seq[T2])(implicit tm: TypeMapper[T2])
    extends OperatorColumn[T2] with UnaryOperator[T1, Seq[T2], T2]
  {
    override def apply(leftValue: T1) = f(leftValue)
    override def operationType = OperationType.SeqMapper
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

  case class CountDistinct(query: ColumnBase[_]) extends SyntheticColumn[Int] {
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
