package revolute.query;

trait QueryModifier

object By {
  def apply(x: Any) = new By(x)
}
class By(x: Any)

sealed abstract class ResultOrdering extends QueryModifier {
  val by: By
  val nullOrdering: ResultOrdering.NullOrdering
  def nullsFirst: ResultOrdering
  def nullsLast: ResultOrdering
}

object ResultOrdering {
  final case class Asc(val by: By, val nullOrdering: ResultOrdering.NullOrdering = ResultOrdering.NullsDefault) extends ResultOrdering {
    override def toString = "ResultOrdering.Asc"
    def nullsFirst = copy(nullOrdering = ResultOrdering.NullsFirst)
    def nullsLast = copy(nullOrdering = ResultOrdering.NullsLast)
  }

  final case class Desc(val by: By, val nullOrdering: ResultOrdering.NullOrdering = ResultOrdering.NullsDefault) extends ResultOrdering {
    override def toString = "ResultOrdering.Desc"
    def nullsFirst = copy(nullOrdering = ResultOrdering.NullsFirst)
    def nullsLast = copy(nullOrdering = ResultOrdering.NullsLast)
  }

  sealed trait NullOrdering
  final case object NullsDefault extends NullOrdering
  final case object NullsFirst extends NullOrdering
  final case object NullsLast extends NullOrdering
}

final case class Grouping(val by: By) extends QueryModifier {
  override def toString = "Grouping"
}
