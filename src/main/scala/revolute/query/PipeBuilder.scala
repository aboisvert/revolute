package revolute.query

import cascading.pipe.{CoGroup, Each, Pipe}
import cascading.operation.Identity
import cascading.tuple.Fields

import revolute.QueryException
import revolute.util._
import scala.collection.mutable.{HashMap, HashSet}

class PipeBuilder(private var _pipe: Pipe) {
  def +=(f: Pipe => Pipe): Unit = {
    val newPipe = f(_pipe)
    Console.println("PipeBuilder: add " + newPipe)
    _pipe = newPipe
  }

  def pipe = _pipe
}
