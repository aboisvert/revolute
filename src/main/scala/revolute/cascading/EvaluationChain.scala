package revolute.cascading

import cascading.tuple.{Fields, TupleEntry}

import revolute.query.{AbstractTable, ColumnBase, ColumnOps, EvaluationContext, NamedColumn, OperationType, Projection}
import revolute.util.{Identifier, NamingContext, Tuple}

import scala.collection._
import scala.collection.mutable.ArrayBuffer

object EvaluationChain {
  def prepare(c: ColumnBase[_], fields: Fields): EvaluationChain = {
    new EvaluationChain(c)
  }

  def inputFields(c: ColumnBase[_]): Fields = {
    val names = new EvaluationChain(c).roots map (_.qualifiedColumnName)
    //Console.println("fields: %s -> %s" format (c, names))
    new Fields(names: _*)
  }

  def outputFields(c: ColumnBase[_]): Fields = {
    var unique = 0

    def name(c: ColumnBase[_]): Option[String] = c match {
      case named: NamedColumn[_] => Some(named.qualifiedColumnName)
      case as: ColumnOps.AsColumnOf[_, _] => name(as.left)
      case _ => None

    }
    val names = new EvaluationChain(c).leaves map { c => name(c).getOrElse {
      unique += 1
      c.nameHint + "#" + unique
    }}
    // Console.println("fields: %s -> %s" format (c, names))
    new Fields(names: _*)
  }

  def tables(c: ColumnBase[_]) = new EvaluationChain(c).roots map (_.table) toSet

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

  val (roots, leaves, context) = {
    def ensureAcyclic(c: ColumnBase[_], path: Seq[ColumnBase[_]] = Seq.empty) {
      for (d <- c.dependencies) {
        if (path contains d) sys.error("Circular dependency detected: " + path)
        ensureAcyclic(d, path :+ d)
      }
    }
    ensureAcyclic(c)

    val roots = {
      val roots = mutable.Set[NamedColumn[_]]()
      def recurse(c: ColumnBase[_]): Unit = c match {
        case named: NamedColumn[_] => roots += named
        case c: ColumnBase[_]      => c.dependencies foreach recurse
      }
      recurse(c)
      roots.toSeq.sortBy(_.qualifiedColumnName)
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
      override def position(c: ColumnBase[_]) = roots.indexOf(c)
      override def toString = "RootContext.EvaluationContext"
    }

    val rootState = new SharedEvaluationContext.SharedState(rootContext)

    val context = {
      val sharedStates = mutable.Map[ColumnBase[_], SharedEvaluationContext.SharedState]()

      roots foreach { r => sharedStates(r) = rootState }

      def recurse(c: ColumnBase[_]): EvaluationContext = {
        if (sharedStates.isDefinedAt(c)) {
          return new SharedEvaluationContext(sharedStates(c))
        }

        val orderedDependencies = c.dependencies.toSeq
        val subContexts = orderedDependencies map recurse toArray

        val subContext = new EvaluationContext {
          lazy val subPositions = subContexts zip orderedDependencies map { case (c, d) => c.position(d) }
          private[this] val tuple = new Tuple {
            val values = new Array[Tuple](orderedDependencies.size)
            def get(pos: Int) = values(pos).get(subPositions(pos))
            override def toString = "SubTuple(%s, %s, %s)" format (orderedDependencies, values.toList, subPositions.toList)
          }
          lazy val positions = orderedDependencies.zipWithIndex.toMap;
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
          override def toString = "EvaluationChainContext(%s)" format subContexts.toList
        }

        var context = c.chainEvaluation(subContext)

        c.operationType match {
          case PureMapper     => // nothing
          case NullableMapper => context = new FilterNull(context)
          case OptionMapper   => context = new FilterNone(context)
          case SeqMapper      => context = new ExplodeSeq(context)
        }

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
      case t: AbstractTable[_] => t.*.columns
      case _                   => Seq(c)
    }

    (roots, leaves, context)
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

class IntermediateEvaluationContext(
  val c: ColumnBase[_],
  val orderedDependencies: Seq[ColumnBase[_]],
  val subContexts: Array[EvaluationContext]
) extends EvaluationContext {

  private[this] lazy val subPositions = subContexts zip orderedDependencies map { case (c, d) => c.position(d) }

  private[this] val tuple = new Tuple {
    val values = new Array[Tuple](orderedDependencies.size)
    def get(pos: Int) = values(pos).get(subPositions(pos))
    override def toString = "SubTuple(%s, %s, %s)" format (orderedDependencies, values.toList, subPositions.toList)
  }

  private[this] lazy val positions = orderedDependencies.zipWithIndex.toMap;

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

class FilterNull(val parent: EvaluationContext) extends EvaluationContext {
  private[this] var tuple: Tuple = _

  override def nextTuple(): Tuple = {
    do {
      tuple = parent.nextTuple()
      if (tuple == null) return null
    } while (tuple.get(0) != null)
    tuple
  }

  override def position(c: ColumnBase[_]) = 0

  override def toString = "FilterNull(%s)" format tuple
}

class FilterNone(val parent: EvaluationContext) extends EvaluationContext {
  private[this] var tuple: SingleValueTuple = new SingleValueTuple()

  override def nextTuple(): Tuple = {
    tuple.value = null
    do {
      val next = parent.nextTuple()
      if (next == null) return null

      val value = next.get(0).asInstanceOf[Option[Any]]
      if (value != null && value != None) {
        tuple.value = value.get
      }
    } while (tuple.value == null)
    tuple
  }

  override def position(c: ColumnBase[_]) = 0

  override def toString = "FilterNone(%s)" format tuple
}

class ExplodeSeq(val parent: EvaluationContext) extends EvaluationContext {
  private[this] var seq: Array[Any] = null
  private[this] var tuple: SingleValueTuple = new SingleValueTuple()
  private[this] var pos = -1

  override def nextTuple(): Tuple = {
    if (seq != null && pos < seq.length) {
      tuple.value = seq(pos)
      pos += 1
      return tuple
    }

    seq = null
    do {
      val next = parent.nextTuple()
      if (next == null) return null

      val value = next.get(0).asInstanceOf[Seq[Any]]
      if (value != null && value.nonEmpty) {
        seq = value.toArray
        pos = 0
      }
    } while (seq == null)
    tuple.value = seq(0)
    tuple
  }

  override def position(c: ColumnBase[_]) = 0

  override def toString = "ExplodeSeq(%s)" format tuple
}

class SingleValueTuple(var value: Any = null) extends Tuple {
  override def get(pos: Int) = value
  override def toString = "Tuple(%s)" format value
}