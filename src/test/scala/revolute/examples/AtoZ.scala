package revolute.examples

import cascading.tap.Hfs
import cascading.scheme.TextDelimited

import revolute._
import revolute.query._
import revolute.query.BasicImplicitConversions._
import revolute.query.QueryBuilder._
import revolute.util._

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

import scala.io.Source

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class AtoZSuite extends WordSpec with ShouldMatchers {

  object AtoZ extends Table[(String, Int)]("A to Z") {
    def letter = column[String]("letter")
    def number = column[Int]("number")
    def * = letter ~ number
  }

  implicit val context = {
    val context = NamingContext()
    context.tableBindings += (AtoZ -> new Hfs(new TextDelimited(AtoZ.*, " "), "target/test/resources/a-z.txt"))
    context
  }

  val sandbox = new Sandbox("target/test/output")

  "simple query" should {
    "map single field" in {
      val result = sandbox run {
        for (az <- AtoZ) yield az.letter
      }
      result.size should be === 26
      result should contain ("a")
      result should contain ("z")
    }

    "map multiple fields" in {
      val result = sandbox run {
        for (az <- AtoZ) yield az.letter ~ az.number
      }

      result.size should be === 26
      result should contain ("a\t1")
      result should contain ("z\t26")
    }

    "filter tuples using where and ===" in {
      val result = sandbox run {
        for {
          az <- AtoZ where (_.letter === "a")
        } yield az.letter ~ az.number
      }
      result.size should be === 1
      result.head should be === ("a\t1")
    }

    "filter tuples using if and ===" in {
      val result = sandbox run {
        for {
          az <- AtoZ if (az.letter === "a")
        } yield az.letter ~ az.number
      }
      result.size should be === 1
      result.head should be === ("a\t1")
    }
  }
}

class Sandbox(val outputDir: String) {

  private var outputNumber = 0

  def outputFile(file: String) = synchronized {
    outputNumber += 1
    file + "-" + outputNumber
  }

  def run[T <: ColumnBase[_]](query: Query[T])(implicit context: NamingContext): Seq[String] = {
    val output = outputFile(outputDir)

    val flow = query.outputTo(new Hfs(new TextDelimited(query.fields, "\t"), output, true))
    flow.start()
    flow.complete()

    val lines = Source.fromFile(output + "/part-00000").getLines.toSeq
    lines
  }
}
