package revolute.query

import revolute.util.Converter
import scala.collection._
import sun.security.pkcs11.P11TlsKeyMaterialGenerator

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
  protected val leftOperand: ColumnBase[P1]
  import ColumnOps._

  def is(e: Column[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
    Is(leftOperand, e)

  def ===(e: Column[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
    Is(leftOperand, e)

  def isNot(e: Column[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
    Not(Is(leftOperand, e))

  def =!=(e: Column[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
    Not(Is(leftOperand, e))

  def <(e: ColumnBase[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
    LessThan(leftOperand, e)

  def <=(e: ColumnBase[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
   LessThanOrEqual(leftOperand, e)

  def >(e: ColumnBase[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
    GreaterThan(leftOperand, e)

  def >=(e: ColumnBase[P1])(implicit o: Ordering[P1]): OperatorColumn[Boolean] =
    GreaterThanOrEqual(leftOperand, e)

  def in(set: Set[P1])(implicit o: scala.math.Ordering[P1]): Column[Boolean] =
    InSet[P1](leftOperand, set)

  def between(start: Column[P1], end: Column[P1])(implicit o: Ordering[P1]): Column[Boolean] =
    Between[P1](leftOperand, start, end)

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

  def as[T2](implicit conv: Converter[P1, T2], tm: TypeMapper[T2]): Column[T2]  = new As[P1, T2](leftOperand)

  // Boolean only
  def &&(b: ColumnBase[P1])(implicit ev: P1 =:= Boolean): Column[Boolean] =
    And(leftOperand.asInstanceOf[ColumnBase[Boolean]], b.asInstanceOf[ColumnBase[Boolean]])

  def ||(b: ColumnBase[P1])(implicit ev: P1 =:= Boolean): Column[Boolean] =
    Or(leftOperand.asInstanceOf[ColumnBase[Boolean]], b.asInstanceOf[ColumnBase[Boolean]])

  def unary_!(implicit ev: P1 =:= Boolean): Column[Boolean] =
    Not(leftOperand.asInstanceOf[ColumnBase[Boolean]])

  // String only
  def length(implicit ev: P1 =:= String): Column[Int] =
    Length(leftOperand.asInstanceOf[ColumnBase[String]])

  def matches(e: String)(implicit ev: P1 =:= String): Column[Boolean] =
    Regex(leftOperand.asInstanceOf[ColumnBase[String]], e)

  def ++(e: Column[P1])(implicit ev: P1 =:= String): Column[String] =
    Concat(leftOperand.asInstanceOf[ColumnBase[String]], e.asInstanceOf[ColumnBase[String]])

  /*
  def startsWith[R](s: String)(implicit om: OptionMapper2[String, String, Boolean, P1, P1, R]): Column[R] =
    om(new StartsWith(leftOperand, s))
  def endsWith[R](s: String)(implicit om: OptionMapper2[String, String, Boolean, P1, P1, R]): Column[R] =
    om(new EndsWith(leftOperand, s))
  */

  def toUpperCase(implicit ev: P1 =:= String): Column[String] =
    ToUpperCase(leftOperand.asInstanceOf[ColumnBase[String]])

  def toLowerCase(implicit ev: P1 =:= String): Column[String] =
    ToLowerCase(leftOperand.asInstanceOf[ColumnBase[String]])

  /*
  def ltrim[R](implicit om: OptionMapper2[String, String, String, P1, P1, R]): Column[R] =
    om(LTrim(leftOperand))
  def rtrim[R](implicit om: OptionMapper2[String, String, String, P1, P1, R]): Column[R] =
    om(RTrim(leftOperand))
  def trim[R](implicit om: OptionMapper2[String, String, String, P1, P1, R]): Column[R] =
    om(LTrim(RTrim(leftOperand)))
  */
}

object ColumnOps {
  // case class In(left: ColumnBase[_], right: ColumnBase[_]) extends OperatorColumn[Boolean] with ColumnOps[Boolean,Boolean]
  case class Count(query: ColumnBase[_]) extends OperatorColumn[Int] with NotAnExpression {
    val name = "count"
    override def tables = query.tables
  }

  case class CountAll(query: ColumnBase[_]) extends OperatorColumn[Int] with NotAnExpression  {
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

  trait UnaryOperator[L, O] extends ColumnBase[O] {
    val left: ColumnBase[L]
    override def arguments = left.arguments
    override def tables = left.tables
  }


  trait BinaryOperator[L, R, O] extends ColumnBase[O] {
    val left: ColumnBase[L]
    val right: ColumnBase[R]

    override def arguments = left.arguments ++ right.arguments
    override def tables = left.tables ++ right.tables
  }

  case class As[T1, T2](val left: ColumnBase[T1])(implicit conv: Converter[T1, T2], tm: TypeMapper[T2])
    extends OperatorColumn[T2] with UnaryOperator[T1, T2]
  {
    override def evaluate(args: Map[String, Any]): T2 = {
      val leftValue = left.evaluate(args)
      conv(leftValue)
    }
  }

  case class LessThan[O: scala.math.Ordering](val left: ColumnBase[O], val right: ColumnBase[O])
    extends OperatorColumn[Boolean] with BinaryOperator[O, O, Boolean]
  {
    override def evaluate(args: Map[String, Any]): Boolean = {
      val l = left.evaluate(args)
      val r = right.evaluate(args)
      implicitly[Ordering[O]] lt (l, r)
    }
  }

  case class LessThanOrEqual[O: scala.math.Ordering](val left: ColumnBase[O], val right: ColumnBase[O])
    extends OperatorColumn[Boolean] with BinaryOperator[O, O, Boolean]
  {
    override def evaluate(args: Map[String, Any]): Boolean = {
      val l = left.evaluate(args)
      val r = right.evaluate(args)
      implicitly[Ordering[O]] lteq (l, r)
    }
    override def tables = left.tables ++ right.tables
  }

  case class GreaterThan[O: scala.math.Ordering](val left: ColumnBase[O], val right: ColumnBase[O])
    extends OperatorColumn[Boolean] with BinaryOperator[O, O, Boolean]
  {
    override def evaluate(args: Map[String, Any]): Boolean = {
      val l = left.evaluate(args)
      val r = right.evaluate(args)
      implicitly[Ordering[O]] gt (l, r)
    }
  }

  case class GreaterThanOrEqual[O: scala.math.Ordering](val left: ColumnBase[O], val right: ColumnBase[O])
    extends OperatorColumn[Boolean] with BinaryOperator[O, O, Boolean]
  {
    override def evaluate(args: Map[String, Any]): Boolean = {
      val l = left.evaluate(args)
      val r = right.evaluate(args)
      implicitly[Ordering[O]] gteq (l, r)
    }
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

  case class Is[O: scala.math.Ordering](left: ColumnBase[O], right: ColumnBase[O])
    extends OperatorColumn[Boolean] with BinaryOperator[O, O, Boolean] with ColumnOps[Boolean,Boolean] {
    override def evaluate(args: Map[String, Any]): Boolean = {
      val l = left.evaluate(args)
      val r = right.evaluate(args)
      implicitly[Ordering[O]] equiv (l, r)
    }
    override def tables = left.tables ++ right.tables
  }

  case class CountDistinct(query: ColumnBase[_]) extends OperatorColumn[Int] with NotAnExpression {
    override def tables = query.tables

  }

  case class InSet[T](query: ColumnBase[T], set: Set[T]) extends OperatorColumn[Boolean] {
    override def arguments = query.arguments
    override def columnName = query.columnName
    override def evaluate(args: Map[String, Any]): Boolean = {
      set contains query.evaluate(args)
    }
    override def tables = query.tables
    override def toString = "InSet(%s, %s)" format (query, set)
  }

  case class Between[O: Ordering](left: ColumnBase[O], start: ColumnBase[O], end: ColumnBase[O])
    extends OperatorColumn[Boolean] with UnaryOperator[O, Boolean] with ColumnOps[Boolean, Boolean]
  {
    override def evaluate(args: Map[String, Any]): Boolean = {
      val leftValue = left.evaluate(args)
      val startValue = start.evaluate(args)
      val endValue = end.evaluate(args)
      ((implicitly[Ordering[O]] gteq (leftValue, startValue)) && (implicitly[Ordering[O]] lteq (leftValue, endValue)))
    }
    override def tables = left.tables
  }

  // Boolean
  case class And(val left: ColumnBase[Boolean], val right: ColumnBase[Boolean]) extends OperatorColumn[Boolean] {
    override def arguments = (left.arguments ++ right.arguments)
    override def evaluate(args: Map[String, Any]): Boolean = {
      if (left.evaluate(args) == false) {
        return false // short circuit
      }
      right.evaluate(args)
    }
    override def tables = left.tables ++ right.tables
  }

  case class Or(left: ColumnBase[Boolean], right: ColumnBase[Boolean]) extends OperatorColumn[Boolean] {
    override def arguments = (left.arguments ++ right.arguments)
    override def evaluate(args: Map[String, Any]): Boolean = {
      if (left.evaluate(args) == true) {
        return true // short circuit
      }
      right.evaluate(args)
    }
    override def tables = left.tables ++ right.tables
  }

  case class Not(left: ColumnBase[Boolean]) extends OperatorColumn[Boolean] with UnaryOperator[Boolean, Boolean] with ColumnOps[Boolean,Boolean] {
    override def evaluate(args: Map[String, Any]): Boolean = {
      !left.evaluate(args)
    }
  }

  // String
  case class Length(left: ColumnBase[String]) extends OperatorColumn[Int] with UnaryOperator[String, Int] with SimpleScalarFunction {
    val name = "length"
    override def evaluate(args: Map[String, Any]): Int = {
      left.evaluate(args).length
    }
  }

  case class ToUpperCase(left: ColumnBase[String]) extends OperatorColumn[String] with UnaryOperator[String, String] with SimpleScalarFunction {
    val name = "ucase"
    override def evaluate(args: Map[String, Any]): String = {
      left.evaluate(args).toUpperCase
    }
  }

  case class ToLowerCase(left: ColumnBase[String]) extends OperatorColumn[String] with UnaryOperator[String, String] with SimpleScalarFunction {
    val name = "lcase"
    override def evaluate(args: Map[String, Any]): String = {
      left.evaluate(args).toUpperCase
    }
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

  case class Regex(left: ColumnBase[String], s: String)
    extends OperatorColumn[Boolean] with UnaryOperator[String, Boolean] with ColumnOps[Boolean,Boolean]
  {
    override def evaluate(args: Map[String, Any]): Boolean = {
      val leftValue = left.evaluate(args)
      leftValue.matches(s)
    }
  }

  case class Concat(left: ColumnBase[String], right: ColumnBase[String])
    extends OperatorColumn[String] with BinaryOperator[String, String, String] with SimpleScalarFunction
  {
    val name = "concat"
    override def evaluate(args: Map[String, Any]): String = {
      val leftValue = left.evaluate(args)
      val rightValue = right.evaluate(args)
      leftValue + rightValue
    }
  }

  /*
  class StartsWith(n: ColumnBase[_], s: String) extends Regex(n, ConstColumn(s, regexEscape(s) + "$"))
  class EndsWith(n: ColumnBase[_], s: String) extends Regex(n, ConstColumn(s, "^" + regexEscape(s)))
  */

  private def regexEscape(s: String) = """\Q""" + s + """\E"""
}
