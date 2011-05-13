package revolute.examples

import cascading.flow.{Flow, FlowConnector}
import cascading.operation.Identity
import cascading.pipe.{Pipe, Each}
import cascading.scheme.TextDelimited
import cascading.tap.Hfs
import cascading.tuple.{Fields, Tuple}

import java.io.File

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

import revolute._
import revolute.query._

import scala.io.Source

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class CascadingSuite extends WordSpec with ShouldMatchers {

  val inputFile = "target/test/resources/a-z.txt"
  val outputDir = "target/test/output/"

  val fields = new Fields("letter", "number")

  "cascading" should {
    "run a simple assembly" in {
      if (!new File(inputFile).exists()) error("data file not found: " + inputFile)

      val source = new Hfs(new TextDelimited(fields, " "), inputFile)
      val sink = new Hfs(new TextDelimited(fields, "\t"), outputDir + "a-z", true)

      var pipe = new Pipe("identity")
      pipe = new Each( pipe, new Identity(fields))

      val flow = new FlowConnector().connect(source, sink, pipe)
      //flow.writeDOT(outputDir + "basic.dot")

      flow.start()
      flow.complete()

      val lines = Source.fromFile(outputDir + "a-z/part-00000").getLines.toSeq
      lines.size should be === 26
      lines should contain ("a\t1")
      lines should contain ("z\t26")
    }
  }
}