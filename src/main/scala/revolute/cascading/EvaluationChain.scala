package revolute.cascading

import cascading.tuple.{Fields, TupleEntry}

import revolute.query._
import revolute.util._

import scala.collection._
import scala.collection.mutable.ArrayBuffer

object EvaluationChain {
  object NoValue extends scala.util.control.ControlThrowable

  def prepare(c: ColumnBase[_], fields: Fields): EvaluationChain = {
    new EvaluationChain(c)
  }

  def inputFields(c: ColumnBase[_]): Fields = {
    val names = new EvaluationChain(c).namedRoots map (_.qualifiedColumnName)
    //Console.println("fields: %s -> %s" format (c, names))
    new Fields(names: _*)
  }

  def outputColumns(c: ColumnBase[_]): Seq[ColumnBase[_]] = new EvaluationChain(c).leaves

  def outputNames(c: ColumnBase[_]): Seq[String] = {
    var unique = 0

    def name(c: ColumnBase[_]): Option[String] = c match {
      case named: NamedColumn[_] => Some(named.qualifiedColumnName)
      case as: ColumnOps.AsColumnOf[_, _] => name(as.left)
      case _ => None

    }
    new EvaluationChain(c).leaves map { c => name(c).getOrElse {
      unique += 1
      c.nameHint + "#" + unique
    }}
  }

  def outputFields(c: ColumnBase[_]): Fields = new Fields(outputNames(c): _*)

  def tables(c: ColumnBase[_]) = new EvaluationChain(c).namedRoots map (_.table) toSet

  def queries(c: ColumnBase[_]): Set[Query[_ <: ColumnBase[_]]] = new EvaluationChain(c).queryRoots map (_.query) toSet

  private class TupleEntryWrapper extends Tuple {
    var tupleEntry: TupleEntry = null
    override def get(pos: Int) = tupleEntry.get(pos)
    override def toString = "TupleEntryWrapper(%s)" format tupleEntry
  }
}

class EvaluationChain(val c: ColumnBase[_]) {
  import EvaluationChain._
  import OperationType._

  private[this] val tuple = new TupleEntryWrapper

  var tupleEntry: TupleEntry = null

  val (namedRoots, queryRoots, leaves, context) = {
    def ensureAcyclic(c: ColumnBase[_], path: Seq[ColumnBase[_]] = Seq.empty) {
      for (d <- c.dependencies) {
        if (path contains d) sys.error("Circular dependency detected: " + path)
        ensureAcyclic(d, path :+ d)
      }
    }
    ensureAcyclic(c)

    val namedRoots = {
      val named = mutable.Set[NamedColumn[_]]()
      def recurse(c: ColumnBase[_]): Unit = c match {
        case c: NamedColumn[_] => named += c
        case c: ColumnBase[_]  => c.dependencies foreach recurse
      }
      recurse(c)
      named.toSeq.sortBy(_.qualifiedColumnName)
    }

    val queryRoots = {
      val queries = mutable.Set[QueryColumn[_, _ <: ColumnBase[_]]]()
      def recurse(c: ColumnBase[_]): Unit = c match {
        case query: QueryColumn[_, _] => queries += query.asInstanceOf[QueryColumn[_, _ <: ColumnBase[_]]]
        case c: ColumnBase[_]         => c.dependencies foreach recurse
      }
      recurse(c)
      queries.toSeq
    }

    //Console.println("roots: " + roots)

    val reverseDependencies = {
      val reverseDependencies = mutable.Map[ColumnBase[_], ArrayBuffer[ColumnBase[_]]]()
      def recurse(c: ColumnBase[_]) {
        for (d <- c.dependencies) {
          reverseDependencies.getOrElseUpdate(d, ArrayBuffer.empty) += c
          recurse(d)
        }
      }
      recurse(c)
      reverseDependencies
    }

    val rootContext = new EvaluationContext {
      override def nextTuple() = {
        //Console.println("root context next tuple")
        if (tupleEntry == null) null
        else {
          tuple.tupleEntry = tupleEntry
          tupleEntry = null
          Console.println("root context next tuple: " + tuple)
          tuple
        }
      }
      override def position(c: ColumnBase[_]) = namedRoots.indexOf(c)
      override def toString = "RootContext.EvaluationContext"
    }

    val rootState = new SharedEvaluationContext.SharedState(rootContext)

    val context = {
      val sharedStates = mutable.Map[ColumnBase[_], SharedEvaluationContext.SharedState]()

      namedRoots foreach { r => sharedStates(r) = rootState }

      def recurse(c: ColumnBase[_]): EvaluationContext = {
        if (sharedStates.isDefinedAt(c)) {
          return new SharedEvaluationContext(sharedStates(c))
        }

        val orderedDependencies = c.dependencies.toSeq
        val subContexts = orderedDependencies map recurse toArray
        val types = orderedDependencies map (_.operationType) toArray

        var context: EvaluationContext = if (types contains OperationType.SeqMapper) {
          new OneToZeroOrManyEvaluationContext(c, orderedDependencies, subContexts, types)
        } else {
          new OneToZeroOrOneEvaluationContext(c, orderedDependencies, subContexts, types)
        }

        context = c.chainEvaluation(context)

        if (reverseDependencies.isDefinedAt(c) && reverseDependencies(c).size > 1) {
          val sharedState = sharedStates.getOrElseUpdate(c, new SharedEvaluationContext.SharedState(context))
          context = new SharedEvaluationContext(sharedState)
        }

        context
      }
      val context = recurse(c)
      //Console.println("<contexts>")
      //Console.println(contexts mkString "\n")
      //Console.println("</contexts>")
      context
    }

    val leaves = c match {
      case p: Projection[_]    => p.columns
      case _                   => Seq(c)
    }

    (namedRoots, queryRoots, leaves, context)
  }

  def arity: Int = c.dependencies.size

  private def flattenProjection(c: ColumnBase[_]) = c match {
    case p: Projection[_] => p.columns
    case _                => Seq(c)
  }
}

object SharedEvaluationContext {
  class SharedState(val parent: EvaluationContext) {
    var tuple: Tuple = _
    var listeners = ArrayBuffer[SharedEvaluationContext]()

    def nextTuple() = {
      tuple = parent.nextTuple()
      listeners foreach { _.consumed = false }
    }

    override def toString = "SharedState(parent=%s)" format parent
  }
}

class SharedEvaluationContext(val shared: SharedEvaluationContext.SharedState) extends EvaluationContext {
  var owner: EvaluationContext = _
  var consumed = false

  shared.listeners += this

  override def nextTuple(): Tuple = {
    Console.println("SharedEvaluationContext.nextTuple() consumed=" + consumed)
    if (consumed || shared.tuple == null) {
      shared.nextTuple()
    }
    consumed = true
    shared.tuple
  }

  override def position(c: ColumnBase[_]) = shared.parent.position(c)

  override def toString = "SharedEvaluationContext(shared=%s)" format shared
}

class OneToZeroOrOneEvaluationContext(
  val c: ColumnBase[_],
  val orderedDependencies: Seq[ColumnBase[_]],
  val subContexts: Array[EvaluationContext],
  val types: Array[OperationType]
) extends EvaluationContext {
  import OperationType._

  private[this] val positions = orderedDependencies.zipWithIndex.toMap

  private[this] val subPositions = subContexts zip orderedDependencies map { case (c, d) => c.position(d) }

  private[this] val tuple = new Tuple {
    val values = new Array[Tuple](orderedDependencies.size)
    def get(pos: Int) = {
      val value = values(pos).get(subPositions(pos))
      if (types(pos) == PureMapper) {
        value
      } else if (types(pos) == NullableMapper) {
        if (value == null) throw EvaluationChain.NoValue
        value
      } else if (types(pos) == OptionMapper) {
        if (value == null) throw EvaluationChain.NoValue
        if (None == value) throw EvaluationChain.NoValue
        value.asInstanceOf[Option[Any]].get
      }
    }
    override def toString = "SubTuple(%s, %s, %s)" format (orderedDependencies, values.toList, subPositions.toList)
  }

  override def nextTuple(): Tuple = {
    //Console.println("EvaluationChainContext nextTuple()")
    var i = 0
    while (i < subContexts.length) {
      val context = subContexts(i)
      val next = context.nextTuple()
      Console.println("next: " + context + " next: " + next)
      if (next == null) return null
      tuple.values(i) = next
      i += 1
    }
    Console.println("EvaluationChainContext nextTuple(): " + tuple)
    tuple
  }

  override def position(c: ColumnBase[_]) = positions(c)

  override def toString = "IntermediateEvaluationContext(%s)" format subContexts.toList
}

class OneToZeroOrManyEvaluationContext(
  val c: ColumnBase[_],
  val orderedDependencies: Seq[ColumnBase[_]],
  val subContexts: Array[EvaluationContext],
  val types: Array[OperationType]
) extends EvaluationContext {
  import OperationType._

  private[this] val positions = orderedDependencies.zipWithIndex.toMap

  private[this] val subPositions = subContexts zip orderedDependencies map { case (c, d) => c.position(d) }


  private[this] val subValues = Array.fill(types.length) { new ArrayBuffer[Any]() }
  private[this] var iterator: Iterator[Seq[Any]] = Iterator.empty

  private[this] var current: Seq[Any] = _

  private[this] val tuple = new Tuple {
    def get(pos: Int) = current(pos)
    override def toString = "OneToZeroOrManyTuple(%s)" format (current)
  }

  override def nextTuple(): Tuple = {
    //Console.println("EvaluationChainContext nextTuple()")
    if (!iterator.hasNext) {
      var i = 0
      while (i < types.length) {
        subValues(i).clear()
        val context = subContexts(i)
        val next = context.nextTuple()
        Console.println("next: " + context + " next: " + next)
        if (next == null) return null

        types(i) match {
          case PureMapper =>
            val value = next.get(0)
            subValues(i) += value

          case NullableMapper =>
            val value = next.get(0)
            if (value != null) subValues(i) += value
            else return null

          case OptionMapper   =>
            val value = next.get(0).asInstanceOf[Option[Any]]
            if (value != null && None != value) subValues(i) += value.get
            else return null

          case SeqMapper      =>
            val values = next.get(0).asInstanceOf[Seq[Any]]
            if (values != null && values.size > 0) subValues(i) ++= values
            else return null
        }

        i += 1
      }
      iterator = Combinations.combinations(subValues)
      current = iterator.next()
      Console.println("EvaluationChainContext nextTuple(): " + tuple)
      tuple
    } else if (iterator.hasNext) {
      current = iterator.next()
      tuple
    } else {
      null
    }
  }

  override def position(c: ColumnBase[_]) = positions(c)

  override def toString = "OneToZeroOrManyEvaluationContext(%s)" format subContexts.toList
}

class SingleValueTuple(var value: Any = null) extends Tuple {
  override def get(pos: Int) = value
  override def toString = "Tuple(%s)" format value
}
