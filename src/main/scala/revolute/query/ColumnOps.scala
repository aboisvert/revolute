package revolute.query

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
  def apply[T : TypeMapper](fname: String): (Seq[Column[_]] => OperatorColumn[T] with SimpleFunction) =
    (paramsC: Seq[Column[_]]) =>
      new OperatorColumn[T] with SimpleFunction {
        val name = fname
      }
}

trait SimpleBinaryOperator {
  val name: String
}

object SimpleBinaryOperator {
  def apply[T : TypeMapper](fname: String): ((Column[_], Column[_]) => OperatorColumn[T] with SimpleBinaryOperator) =
    (leftC: Column[_], rightC: Column[_]) =>
      new OperatorColumn[T] with SimpleBinaryOperator {
        val name = fname
      }
}

trait SimpleScalarFunction

trait ColumnOps[B1, P1] {
  protected val leftOperand: ColumnBase[_]
  import ColumnOps._

  def is[P2, R](e: Column[P2])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P2, R]): Column[R] =
    om(Is(leftOperand, e))
  def === [P2, R](e: Column[P2])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P2, R]): Column[R] =
    om(Is(leftOperand, e))
  def isNot[P2, R](e: Column[P2])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P2, R]): Column[R] =
    om(Not(Is(leftOperand, e)))
  def =!= [P2, R](e: Column[P2])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P2, R]): Column[R] =
    om(Not(Is(leftOperand, e)))
  def < [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P2, R]): Column[R] =
    om(Relational("<", leftOperand, e))
  def <= [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P2, R]): Column[R] =
    om(Relational("<=", leftOperand, e))
  def > [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P2, R]): Column[R] =
    om(Relational(">", leftOperand, e))
  def >= [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P2, R]): Column[R] =
    om(Relational(">=", leftOperand, e))
  def inSet[R](seq: Seq[B1])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P1, R], tm: BaseTypeMapper[B1]): Column[R] =
    om(InSet(leftOperand, seq, tm, false))
  def inSetBind[R](seq: Seq[B1])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P1, R], tm: BaseTypeMapper[B1]): Column[R] =
    om(InSet(leftOperand, seq, tm, true))
  def between[P2, P3, R](start: Column[P2], end: Column[P3])(implicit om: OptionMapper3[B1, B1, B1, Boolean, P1, P2, P3, R]): Column[R] =
    om(Between(leftOperand, start, end))
  //def ifNull[B2, P2, R](e: Column[P2])(implicit om: OptionMapper2[B1, B2, Boolean, P1, P2, R]): Column[P2] =
  //  IfNull(leftOperand, e)
  def min(implicit om: OptionMapper2[B1, B1, B1, Option[B1], Option[B1], Option[B1]], tm: BaseTypeMapper[B1]): Column[Option[B1]] =
    om(Min(leftOperand, tm))
  def max(implicit om: OptionMapper2[B1, B1, B1, Option[B1], Option[B1], Option[B1]], tm: BaseTypeMapper[B1]): Column[Option[B1]] =
    om(Max(leftOperand, tm))

  // NumericTypeMapper only
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

  // Boolean only
  def &&[P2, R](b: ColumnBase[P2])(implicit om: OptionMapper2[Boolean, Boolean, Boolean, P1, P2, R]): Column[R] =
    om(And(leftOperand, b))
  def ||[P2, R](b: ColumnBase[P2])(implicit om: OptionMapper2[Boolean, Boolean, Boolean, P1, P2, R]): Column[R] =
    om(Or(leftOperand, b))
  def unary_![R](implicit om: OptionMapper2[Boolean, Boolean, Boolean, P1, P1, R]): Column[R] =
    om(Not(leftOperand))

  // String only
  def length[R](implicit om: OptionMapper2[String, String, Int, P1, P1, R]): Column[R] =
    om(Length(leftOperand))
  
  def matches[P2, R](e: Column[P2])(implicit om: OptionMapper2[String, String, Boolean, P1, P2, R]): Column[R] =
    om(Regex(leftOperand, e))
  def regex[P2, R](e: Column[P2])(implicit om: OptionMapper2[String, String, Boolean, P1, P2, R]): Column[R] =
    om(Regex(leftOperand, e))
  def regex[P2, R](e: Column[P2], esc: Char)(implicit om: OptionMapper2[String, String, Boolean, P1, P2, R]): Column[R] =
    om(Regex(leftOperand, e))
  def ++[P2, R](e: Column[P2])(implicit om: OptionMapper2[String, String, String, P1, P2, R]): Column[R] =
    om(Concat(leftOperand, e))
  def startsWith[R](s: String)(implicit om: OptionMapper2[String, String, Boolean, P1, P1, R]): Column[R] =
    om(new StartsWith(leftOperand, s))
  def endsWith[R](s: String)(implicit om: OptionMapper2[String, String, Boolean, P1, P1, R]): Column[R] =
    om(new EndsWith(leftOperand, s))
  def toUpperCase[R](implicit om: OptionMapper2[String, String, String, P1, P1, R]): Column[R] =
    om(ToUpperCase(leftOperand))
  def toLowerCase[R](implicit om: OptionMapper2[String, String, String, P1, P1, R]): Column[R] =
    om(ToLowerCase(leftOperand))
  def ltrim[R](implicit om: OptionMapper2[String, String, String, P1, P1, R]): Column[R] =
    om(LTrim(leftOperand))
  def rtrim[R](implicit om: OptionMapper2[String, String, String, P1, P1, R]): Column[R] =
    om(RTrim(leftOperand))
  def trim[R](implicit om: OptionMapper2[String, String, String, P1, P1, R]): Column[R] =
    om(LTrim(RTrim(leftOperand)))
}

object ColumnOps {
  // case class In(left: ColumnBase[_], right: ColumnBase[_]) extends OperatorColumn[Boolean] with ColumnOps[Boolean,Boolean]
  case class Count(query: ColumnBase[_]) extends OperatorColumn[Int] { val name = "count" }
  case class CountAll(child: ColumnBase[_]) extends OperatorColumn[Int]
  case class Mod[T](left: ColumnBase[_], right: ColumnBase[_], tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleScalarFunction { val name = "mod" }
  case class Abs[T](query: ColumnBase[_], tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleScalarFunction { val name = "abs" }
  case class Ceil[T](query: ColumnBase[_], tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleScalarFunction { val name = "ceiling" }
  case class Floor[T](query: ColumnBase[_], tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleScalarFunction { val name = "floor" }
  case class Sign(query: ColumnBase[_]) extends OperatorColumn[Int] with SimpleScalarFunction { val name = "sign" }
  case class Avg[T](query: ColumnBase[_], tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleFunction { val name = "avg" }
  case class Min[T](query: ColumnBase[_], tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleFunction { val name = "min" }
  case class Max[T](query: ColumnBase[_], tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleFunction { val name = "max" }
  case class Sum[T](query: ColumnBase[_], tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleFunction { val name = "sum" }
  case class Relational(name: String, left: ColumnBase[_], right: ColumnBase[_]) extends OperatorColumn[Boolean] with SimpleBinaryOperator with ColumnOps[Boolean,Boolean]
  case class Exists(query: ColumnBase[_]) extends OperatorColumn[Boolean] with SimpleFunction with ColumnOps[Boolean,Boolean] { val name = "exists" }
  case class Arith[T : TypeMapper](name: String, left: ColumnBase[_], right: ColumnBase[_]) extends OperatorColumn[T] with SimpleBinaryOperator
  case class IfNull(left: ColumnBase[_], right: ColumnBase[_]) extends SimpleScalarFunction { val name = "ifNull" }

  case class Is(left: ColumnBase[_], right: ColumnBase[_]) extends OperatorColumn[Boolean] with ColumnOps[Boolean,Boolean]
  case class CountDistinct(query: ColumnBase[_]) extends OperatorColumn[Int]
  case class InSet[T](query: ColumnBase[_], seq: Seq[T], tm: TypeMapper[T], bind: Boolean) extends OperatorColumn[Boolean]

  case class Between(left: ColumnBase[_], start: Any, end: Any) extends OperatorColumn[Boolean] with ColumnOps[Boolean,Boolean] {
    def nodeChildren = left :: start :: end :: Nil
  }

  case class AsColumnOf[T : TypeMapper](query: ColumnBase[_], typeName: Option[String]) extends Column[T]

  // Boolean
  case class And(left: ColumnBase[_], right: ColumnBase[_]) extends OperatorColumn[Boolean] with SimpleBinaryOperator with ColumnOps[Boolean,Boolean] { val name = "and" }
  case class Or(left: ColumnBase[_], right: ColumnBase[_]) extends OperatorColumn[Boolean] with SimpleBinaryOperator with ColumnOps[Boolean,Boolean] { val name = "or" }
  case class Not(query: ColumnBase[_]) extends OperatorColumn[Boolean] with ColumnOps[Boolean,Boolean]

  // String
  case class Length(query: ColumnBase[_]) extends OperatorColumn[Int] with SimpleScalarFunction { val name = "length" }
  case class ToUpperCase(query: ColumnBase[_]) extends OperatorColumn[String] with SimpleScalarFunction { val name = "ucase" }
  case class ToLowerCase(query: ColumnBase[_]) extends OperatorColumn[String] with SimpleScalarFunction { val name = "lcase" }
  case class LTrim(query: ColumnBase[_]) extends OperatorColumn[String] with SimpleScalarFunction { val name = "ltrim" }
  case class RTrim(query: ColumnBase[_]) extends OperatorColumn[String] with SimpleScalarFunction { val name = "rtrim" }
  case class Regex(left: ColumnBase[_], right: ColumnBase[_]) extends OperatorColumn[Boolean] with ColumnOps[Boolean,Boolean]
  case class Concat(left: ColumnBase[_], right: ColumnBase[_]) extends OperatorColumn[String] with SimpleScalarFunction { val name = "concat" }
  class StartsWith(n: ColumnBase[_], s: String) extends Regex(n, ConstColumn(s, regexEscape(s) + "$"))
  class EndsWith(n: ColumnBase[_], s: String) extends Regex(n, ConstColumn(s, "^" + regexEscape(s)))
  
  private def regexEscape(s: String) = """\Q""" + s + """\E"""
}