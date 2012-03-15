package revolute.util

import scala.collection.mutable.ArrayBuffer

/**
 * Utility method to generate combinations
 */
object Combinations {

  /** Returns the factorial of n */
  def factorial(n: Int): Long = {
    if (n > 20) {
      throw new IllegalArgumentException("Factorial with n > 20 results in long integer overflow")
    }
    var fact = 1L
    var i = n
    while (i > 1) {
      fact = fact * i
      i -= 1
    }
    fact
  }

  /** Generates all combinations of n elements, taken r at a time */
  def combinations(n: Int, r: Int): Combinations = new Combinations(n, r)

  /**
   * Generate all combinations of values.size elements by taking one element from each of values' sequences.
   *
   * e.g.
   *  combinations(Seq(Seq(A1, A2), Seq(B))) => Seq(Seq(A1, B), Seq(A2, B))
   *  combinations(Seq(Seq(A1, A2), Seq(B1, B2))) => Seq(Seq(A1, B1), Seq(A1, B2), Seq(A2, B1), Seq(A2, B2))
   */
  def combinations[T](values: IndexedSeq[IndexedSeq[T]]): Iterator[IndexedSeq[T]] = new Iterator[IndexedSeq[T]] {
    private val _indexes = new Array[Int](values.size)
    private var _hasNext = true
    private var _next = values map (_.head)
    private var _pos = 0

    override def hasNext = _hasNext

    override def next(): IndexedSeq[T] = {
      val saved = _next
      while (_pos < values.size) {
        if (_indexes(_pos) < values(_pos).size-1) {
          _indexes(_pos) += 1
          var x = 0
          while (x < _pos) {
            _indexes(x) = 0
            x += 1
          }
          _next = _indexes.zipWithIndex.map { case (i, j) => values(j)(i) }
          _pos = 0
          return saved
        } else {
          _pos += 1
        }
      }
      _hasNext = false
      saved
    }
  }

  /**
   * Returns a combinations iterator when sequence of values are taken "size" at a time.
   *
   * e.g.  combinations(ABC, 2) => Seq(AB, AC, BC)
   */
  def combinations[T](values: IndexedSeq[T], size: Int): Iterator[IndexedSeq[T]] = {
    // This should be an anonymous class but Scala 2.8.0RC3; workaround for
    class AnonIterator(values: IndexedSeq[T], size: Int) extends Iterator[IndexedSeq[T]] {
      private val _iter = new Combinations(values.size, size)

      override def hasNext = _iter.hasNext
      override def next()  = _iter.next() map values
    }
    new AnonIterator(values, size)
  }

  /**
   * Returns all flattened elements following the given element, excluding
   * elements in the same position as the provided element.
   *
   * e.g. following(C, Seq(AB, CD, E, FG) => Seq(E, F, G)
   */
  def following[T](current: T, values: IndexedSeq[IndexedSeq[T]]): IndexedSeq[T] = {
    val pos = values indexWhere (_ contains current)
    if (pos == -1) {
      throw new IllegalArgumentException("Current value %s not found in %s" format (current, values))
    }
    values drop (pos+1) flatten
  }
}

/**
 * Generates all combinations of n elements, taken r at a time.
 *
 * The algorithm is described by Kenneth H. Rosen, Discrete Mathematics and Its
 * Applications, 4th edition.
 */
class Combinations(val n: Int, val r: Int) extends Iterator[IndexedSeq[Int]] {
  import Combinations._

  if (n <= 0) {
    throw new IllegalArgumentException("n must be greater or equal to 1")
  }

  if (r <= 0) {
    throw new IllegalArgumentException("r must be greater or equal to 1")
  }

  if (r > n) {
    throw new IllegalArgumentException("r must be less than or equal to n")
  }

  /** Internal array used for generating combinations */
  private val a = Array.tabulate(r) { identity }

  /** Total number of combinations */
  val total = factorial(n) / ( factorial(r) * factorial(n-r))

  /** Number of combinations left */
  private var _numLeft: Long = total

  /** Returns the number of combinations left */
  def numLeft: Long = _numLeft

  override def hasNext = _numLeft > 0

  override def next() = {
    if (_numLeft != total) {
      var i = r - 1
      while (a(i) == n - r + i) {
        i -= 1
      }
      a(i) += 1

      var j = i + 1
      while (j < r) {
        a(j) = a(i) + j - i
        j += 1
      }

    }

    _numLeft -= 1

    a.clone
  }
}
