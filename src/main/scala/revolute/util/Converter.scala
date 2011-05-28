package revolute.util

@serializable trait Converter[From, To] extends Function1[From, To]

object Converters {
  implicit val stringToInt = new Converter[String, Int] {
    def apply(x: String) = {
      if (x eq null) null.asInstanceOf[Int] else x.toInt
    }
  }

  implicit val intToString = new Converter[Int, String] {
    def apply(x: Int) = {
      if (x.asInstanceOf[AnyRef] eq null) null.asInstanceOf[String] else x.toString
    }
  }
}
