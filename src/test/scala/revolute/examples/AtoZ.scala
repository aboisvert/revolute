package revolute.examples

import cascading.tap.Hfs
import cascading.scheme.TextDelimited

import revolute.query._
import revolute.query.BasicImplicitConversions._
import revolute.query.QueryBuilder._
import revolute.util._
import revolute.util.Converters._

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

import scala.io.Source

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class AtoZSuite extends WordSpec with ShouldMatchers {

  object AtoZ extends Table[(String, Int)]("A to Z") {
    def letter = column[String]("letter")
    def number = column[String]("number").as[Int]
    def * = letter ~ number
  }

  object Words extends Table[(String, String)]("Words") {
    def letter = column[String]("letter")
    def word = column[String]("word")
    def * = letter ~ word
  }

  def context(tables: AbstractTable[_]*) = {
    implicit val context = NamingContext()
    if (tables contains AtoZ)
      context.tableBindings += (AtoZ -> new Hfs(new TextDelimited(AtoZ.*, " "), "target/test/resources/a-z.txt"))
    if (tables contains Words)
      context.tableBindings += (Words -> new Hfs(new TextDelimited(Words.*, " "), "target/test/resources/words.txt"))
    context
  }

  val sandbox = new Sandbox("target/test/output")

  "simple query" should {
    "map single field" in {
      implicit val _ = context(AtoZ)

      val result = sandbox run {
        for (az <- AtoZ) yield az.letter
      }
      result.size should be === 26
      result should contain (Seq("a"))
      result should contain (Seq("z"))
    }

    "map multiple fields" in {
      implicit val _ = context(AtoZ)

      val result = sandbox run {
        for (az <- AtoZ) yield az.letter ~ az.number
      }
      result.size should be === 26
      result should contain (Seq("a", "1"))
      result should contain (Seq("z", "26"))
    }

    "filter tuples using where and ===" in {
      implicit val _ = context(AtoZ)

      val result = sandbox run {
        for {
          az <- AtoZ where (_.letter === "a")
        } yield az.letter ~ az.number
      }
      result should be === Seq(
        Seq("a", "1")
      )
    }

    "filter tuples using if and ===" in {
      implicit val _ = context(AtoZ)

      val result = sandbox run {
        for {
          az <- AtoZ if (az.letter === "a")
        } yield az.letter ~ az.number
      }
      result should be === Seq(
        Seq("a", "1")
      )
    }

    "concatenate fields and coerse field to string" in {
      implicit val _ = context(AtoZ)

      val result = sandbox run {
        for {
          concat <- AtoZ.letter ++ AtoZ.number.as[String]
        } yield concat
      }
      result.size should be === 26
      result should contain (Seq("a1"))
      result should contain (Seq("b2"))
      result should contain (Seq("c3"))
    }

    "filter tuples with a set" in {
      implicit val _ = context(AtoZ)

      val result = sandbox run {
        for {
          abc <- AtoZ where (_.letter in Set("a", "b", "c"))
        } yield abc
      }
      result.size should be === 3
      result should contain (Seq("a", "1"))
      result should contain (Seq("b", "2"))
      result should contain (Seq("c", "3"))
    }

    "apply multiple conditions" in {
      implicit val _ = context(AtoZ)

      val result = sandbox run {
        for {
          _ <- AtoZ where (_.letter in Set("a", "b"))
          _ <- AtoZ where (_.letter in Set("a"))
        } yield AtoZ
      }
      result.size should be === 1
      result should contain (Seq("a", "1"))
    }

    "filter using multiple conditions with logical AND" in {
      implicit val _ = context(AtoZ)

      val result = sandbox run {
        for {
          _ <- AtoZ where { az => az.number > 5 && az.number <= 7 }
        } yield AtoZ
      }
      result.size should be === 2
      result should contain (Seq("f", "6"))
      result should contain (Seq("g", "7"))
    }

    "join tables implicitly" in {
      implicit val _ = context(AtoZ, Words)

      val result = sandbox run {
        for {
          az <- AtoZ
          words <- Words if az.letter is words.letter
        } yield az.letter ~ az.number ~ words.word
      }
      result.size should be === 8
      result should contain (Seq("a", "1", "apple"))
      result should contain (Seq("h", "8", "house"))
    }

    "join tables using innerJoin" in {
      implicit val _ = context(AtoZ, Words)

      val result = sandbox run {
        // fails to compile?
        // inferred type arguments [Boolean] do not conform to method filter's type parameter bounds
        // [T <: revolute.query.ColumnBase[_]]
        /*
        for {
          Join(az, words) <- ((AtoZ innerJoin Words) on (_.letter is _.letter))
        } yield az.letter ~ az.number ~ words.word
        */
        AtoZ innerJoin Words on (_.letter is _.letter) map { case Join(az, words) =>
          az.letter ~ az.number ~ words.word
        }
      }
      result.size should be === 8
      result should contain (Seq("a", "1", "apple"))
      result should contain (Seq("h", "8", "house"))
    }
  }
}

class Sandbox(val outputDir: String) {

  private var outputNumber = 0

  def outputFile(file: String) = synchronized {
    outputNumber += 1
    file + "-" + outputNumber
  }

  def run[T <: ColumnBase[_]](query: Query[T])(implicit context: NamingContext): Seq[Seq[String]] = {
    val output = outputFile(outputDir)

    val flow = query.outputTo(new Hfs(new TextDelimited(query.fields, "\t"), output, true))
    flow.start()
    flow.complete()

    val lines = Source.fromFile(output + "/part-00000").getLines.toSeq map (_.split("\t").toSeq)
    lines
  }
}
