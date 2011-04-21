package revolute.query

import revolute.util.{Node, UnaryNode, BinaryNode}

abstract class ColumnOption[+T]

object ColumnOptions {
  case object NotNull extends ColumnOption[Nothing]
  case object Nullable extends ColumnOption[Nothing]
  case class Default[T](val defaultValue: T) extends ColumnOption[T]
}

trait SimpleFunction extends Node {
  val name: String
}

object SimpleFunction {
  def apply[T : TypeMapper](fname: String): (Seq[Column[_]] => OperatorColumn[T] with SimpleFunction) =
    (paramsC: Seq[Column[_]]) =>
      new OperatorColumn[T] with SimpleFunction {
        val name = fname
        def nodeChildren = paramsC.map(n => Node(n)).toList
      }
}

trait SimpleBinaryOperator extends BinaryNode {
  val name: String
}

object SimpleBinaryOperator {
  def apply[T : TypeMapper](fname: String): ((Column[_], Column[_]) => OperatorColumn[T] with SimpleBinaryOperator) =
    (leftC: Column[_], rightC: Column[_]) =>
      new OperatorColumn[T] with SimpleBinaryOperator {
        val name = fname
        val left = Node(leftC)
        val right = Node(rightC)
      }
}

trait SimpleScalarFunction extends Node

trait ColumnOps[B1, P1] {
  protected val leftOperand: Node
  import ColumnOps._

  def is[P2, R](e: Column[P2])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P2, R]): Column[R] =
    om(Is(leftOperand, Node(e)))
  def === [P2, R](e: Column[P2])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P2, R]): Column[R] =
    om(Is(leftOperand, Node(e)))
  def isNot[P2, R](e: Column[P2])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P2, R]): Column[R] =
    om(Not(Is(leftOperand, Node(e))))
  def =!= [P2, R](e: Column[P2])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P2, R]): Column[R] =
    om(Not(Is(leftOperand, Node(e))))
  def < [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P2, R]): Column[R] =
    om(Relational("<", leftOperand, Node(e)))
  def <= [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P2, R]): Column[R] =
    om(Relational("<=", leftOperand, Node(e)))
  def > [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P2, R]): Column[R] =
    om(Relational(">", leftOperand, Node(e)))
  def >= [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P2, R]): Column[R] =
    om(Relational(">=", leftOperand, Node(e)))
  def inSet[R](seq: Seq[B1])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P1, R], tm: BaseTypeMapper[B1]): Column[R] =
    om(InSet(leftOperand, seq, tm, false))
  def inSetBind[R](seq: Seq[B1])(implicit om: OptionMapper2[B1, B1, Boolean, P1, P1, R], tm: BaseTypeMapper[B1]): Column[R] =
    om(InSet(leftOperand, seq, tm, true))
  def between[P2, P3, R](start: Column[P2], end: Column[P3])(implicit om: OptionMapper3[B1, B1, B1, Boolean, P1, P2, P3, R]): Column[R] =
    om(Between(leftOperand, start, end))
  def ifNull[B2, P2, R](e: Column[P2])(implicit om: OptionMapper2[B1, B2, Boolean, P1, P2, R]): Column[P2] =
    e.mapOp(IfNull(leftOperand, _))
  def min(implicit om: OptionMapper2[B1, B1, B1, Option[B1], Option[B1], Option[B1]], tm: BaseTypeMapper[B1]): Column[Option[B1]] =
    om(Min(leftOperand, tm))
  def max(implicit om: OptionMapper2[B1, B1, B1, Option[B1], Option[B1], Option[B1]], tm: BaseTypeMapper[B1]): Column[Option[B1]] =
    om(Max(leftOperand, tm))

  // NumericTypeMapper only
  def + [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, B1, P1, P2, R], tm: BaseTypeMapper[B1] with NumericTypeMapper): Column[R] =
    om(Arith[B1]("+", leftOperand, Node(e)))
  def - [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, B1, P1, P2, R], tm: BaseTypeMapper[B1] with NumericTypeMapper): Column[R] =
    om(Arith[B1]("-", leftOperand, Node(e)))
  def * [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, B1, P1, P2, R], tm: BaseTypeMapper[B1] with NumericTypeMapper): Column[R] =
    om(Arith[B1]("*", leftOperand, Node(e)))
  def / [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, B1, P1, P2, R], tm: BaseTypeMapper[B1] with NumericTypeMapper): Column[R] =
    om(Arith[B1]("/", leftOperand, Node(e)))
  def % [P2, R](e: ColumnBase[P2])(implicit om: OptionMapper2[B1, B1, B1, P1, P2, R], tm: BaseTypeMapper[B1] with NumericTypeMapper): Column[R] =
    om(Mod[B1](leftOperand, Node(e), tm))
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
    om(And(leftOperand, Node(b)))
  def ||[P2, R](b: ColumnBase[P2])(implicit om: OptionMapper2[Boolean, Boolean, Boolean, P1, P2, R]): Column[R] =
    om(Or(leftOperand, Node(b)))
  def unary_![R](implicit om: OptionMapper2[Boolean, Boolean, Boolean, P1, P1, R]): Column[R] =
    om(Not(leftOperand))

  // String only
  def length[R](implicit om: OptionMapper2[String, String, Int, P1, P1, R]): Column[R] =
    om(Length(leftOperand))
  
  def matches[P2, R](e: Column[P2])(implicit om: OptionMapper2[String, String, Boolean, P1, P2, R]): Column[R] =
    om(Regex(leftOperand, Node(e)))
  def regex[P2, R](e: Column[P2])(implicit om: OptionMapper2[String, String, Boolean, P1, P2, R]): Column[R] =
    om(Regex(leftOperand, Node(e)))
  def regex[P2, R](e: Column[P2], esc: Char)(implicit om: OptionMapper2[String, String, Boolean, P1, P2, R]): Column[R] =
    om(Regex(leftOperand, Node(e)))
  def ++[P2, R](e: Column[P2])(implicit om: OptionMapper2[String, String, String, P1, P2, R]): Column[R] =
    om(Concat(leftOperand, Node(e)))
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
  // case class In(left: Node, right: Node) extends OperatorColumn[Boolean] with ColumnOps[Boolean,Boolean]
  case class Count(child: Node) extends OperatorColumn[Int] with UnaryNode { val name = "count" }
  case class CountAll(child: Node) extends OperatorColumn[Int] with UnaryNode
  case class Mod[T](left: Node, right: Node, tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleScalarFunction with BinaryNode { val name = "mod" }
  case class Abs[T](child: Node, tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleScalarFunction with UnaryNode { val name = "abs" }
  case class Ceil[T](child: Node, tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleScalarFunction with UnaryNode { val name = "ceiling" }
  case class Floor[T](child: Node, tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleScalarFunction with UnaryNode { val name = "floor" }
  case class Sign(child: Node) extends OperatorColumn[Int] with SimpleScalarFunction with UnaryNode { val name = "sign" }
  case class Avg[T](child: Node, tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleFunction with UnaryNode { val name = "avg" }
  case class Min[T](child: Node, tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleFunction with UnaryNode { val name = "min" }
  case class Max[T](child: Node, tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleFunction with UnaryNode { val name = "max" }
  case class Sum[T](child: Node, tm: TypeMapper[T]) extends OperatorColumn[T]()(tm) with SimpleFunction with UnaryNode { val name = "sum" }
  case class Relational(name: String, left: Node, right: Node) extends OperatorColumn[Boolean] with SimpleBinaryOperator with ColumnOps[Boolean,Boolean]
  case class Exists(child: Node) extends OperatorColumn[Boolean] with SimpleFunction with UnaryNode with ColumnOps[Boolean,Boolean] { val name = "exists" }
  case class Arith[T : TypeMapper](name: String, left: Node, right: Node) extends OperatorColumn[T] with SimpleBinaryOperator
  case class IfNull(left: Node, right: Node) extends SimpleScalarFunction with BinaryNode { val name = "ifNull" }

  case class Is(left: Node, right: Node) extends OperatorColumn[Boolean] with BinaryNode with ColumnOps[Boolean,Boolean]
  case class CountDistinct(child: Node) extends OperatorColumn[Int] with UnaryNode
  case class InSet[T](child: Node, seq: Seq[T], tm: TypeMapper[T], bind: Boolean) extends OperatorColumn[Boolean] with UnaryNode

  case class Between(left: Node, start: Node, end: Node) extends OperatorColumn[Boolean] with ColumnOps[Boolean,Boolean] {
    def nodeChildren = left :: start :: end :: Nil
  }

  case class AsColumnOf[T : TypeMapper](child: Node, typeName: Option[String]) extends Column[T] with UnaryNode

  // Boolean
  case class And(left: Node, right: Node) extends OperatorColumn[Boolean] with SimpleBinaryOperator with ColumnOps[Boolean,Boolean] { val name = "and" }
  case class Or(left: Node, right: Node) extends OperatorColumn[Boolean] with SimpleBinaryOperator with ColumnOps[Boolean,Boolean] { val name = "or" }
  case class Not(child: Node) extends OperatorColumn[Boolean] with UnaryNode with ColumnOps[Boolean,Boolean]

  // String
  case class Length(child: Node) extends OperatorColumn[Int] with SimpleScalarFunction with UnaryNode { val name = "length" }
  case class ToUpperCase(child: Node) extends OperatorColumn[String] with SimpleScalarFunction with UnaryNode { val name = "ucase" }
  case class ToLowerCase(child: Node) extends OperatorColumn[String] with SimpleScalarFunction with UnaryNode { val name = "lcase" }
  case class LTrim(child: Node) extends OperatorColumn[String] with SimpleScalarFunction with UnaryNode { val name = "ltrim" }
  case class RTrim(child: Node) extends OperatorColumn[String] with SimpleScalarFunction with UnaryNode { val name = "rtrim" }
  case class Regex(left: Node, right: Node) extends OperatorColumn[Boolean] with BinaryNode with ColumnOps[Boolean,Boolean]
  case class Concat(left: Node, right: Node) extends OperatorColumn[String] with SimpleScalarFunction with BinaryNode { val name = "concat" }
  class StartsWith(n: Node, s: String) extends Regex(n, ConstColumn(s, regexEscape(s) + "$"))
  class EndsWith(n: Node, s: String) extends Regex(n, ConstColumn(s, "^" + regexEscape(s)))
  
  private def regexEscape(s: String) = """\Q""" + s + """\E"""
}