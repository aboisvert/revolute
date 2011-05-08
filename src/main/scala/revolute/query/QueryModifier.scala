package revolute.query;

trait QueryModifier

object By {
  def apply(x: Any) = new By(x)
}
class By(x: Any)

sealed abstract class Ordering extends QueryModifier {
  val by: By
  val nullOrdering: Ordering.NullOrdering
  def nodeChildren = by :: Nil
  def nullsFirst: Ordering
  def nullsLast: Ordering
}

object Ordering {
  final case class Asc(val by: By, val nullOrdering: Ordering.NullOrdering = Ordering.NullsDefault) extends Ordering {
    override def toString = "Ordering.Asc"
    def nullsFirst = copy(nullOrdering = Ordering.NullsFirst)
    def nullsLast = copy(nullOrdering = Ordering.NullsLast)
  }

  final case class Desc(val by: By, val nullOrdering: Ordering.NullOrdering = Ordering.NullsDefault) extends Ordering {
    override def toString = "Ordering.Desc"
    def nullsFirst = copy(nullOrdering = Ordering.NullsFirst)
    def nullsLast = copy(nullOrdering = Ordering.NullsLast)
  }

  sealed trait NullOrdering
  final case object NullsDefault extends NullOrdering
  final case object NullsFirst extends NullOrdering
  final case object NullsLast extends NullOrdering
}

final case class Grouping(val by: By) extends QueryModifier {
  def nodeChildren = by :: Nil
  override def toString = "Grouping"
}