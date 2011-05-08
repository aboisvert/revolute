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
  
  "simple query" should {
    "map single field" in {
      val query = for (az <- AtoZ) yield az.letter
      val output = "target/test/output/letter_only"

      val flow = query.outputTo(new Hfs(new TextDelimited(query.fields, "\t"), output, true))
      flow.start()
      flow.complete()
      
      val lines = Source.fromFile(output + "/part-00000").getLines.toSeq
      lines.size should be === 26
      lines should contain ("a")
      lines should contain ("z")
    }

    "map multiple fields" in {
      val query = for (az <- AtoZ) yield az.letter ~ az.number
      val output = "target/test/output/letter_and_number"

      val flow = query.outputTo(new Hfs(new TextDelimited(query.fields, "\t"), output, true))
      flow.start()
      flow.complete()
      
      val lines = Source.fromFile(output + "/part-00000").getLines.toSeq
      lines.size should be === 26
      lines should contain ("a\t1")
      lines should contain ("z\t26")
    }

    "filter tuples" in {
      val query = for {
        az <- AtoZ where (_.letter === "a")
      } yield az.letter ~ az.number
      val output = "target/test/output/letter_and_number"

      val flow = query.outputTo(new Hfs(new TextDelimited(query.fields, "\t"), output, true))
      flow.start()
      flow.complete()
      
      val lines = Source.fromFile(output + "/part-00000").getLines.toSeq
      lines.size should be === 1
      lines.head should be === ("a\t1")
    }
  }
}