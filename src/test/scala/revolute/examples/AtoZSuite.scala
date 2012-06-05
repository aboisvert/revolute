package revolute.examples

import cascading.flow.Flow
import cascading.tap.SinkMode
import cascading.tap.local.FileTap
import cascading.tuple.Tuple
import cascading.scheme.local.TextDelimited

import revolute.flow._
import revolute.query._
import revolute.query.ImplicitConversions._
import revolute.util._
import revolute.util.Compat._
import revolute.util.Converters._
import revolute.test.Sandbox

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

import scala.collection._
import scala.collection.mutable.ArrayBuffer

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class AtoZSuite extends WordSpec with ShouldMatchers {

  object AtoZ extends Table {
    def letter = column[String]("letter")
    def number = column[String]("number").asColumnOf[Int]
    def * = letter ~ number
  }

  object Words extends Table {
    def letter = column[String]("letter")
    def word = column[String]("word")
    def * = letter ~ word
  }

  implicit val context = FlowContext.local { context =>
    context.sources(AtoZ)  = () => new FileTap(new TextDelimited(inputFields(AtoZ.*), " "),  "target/test/resources/a-z.txt")
    context.sources(Words) = () => new FileTap(new TextDelimited(inputFields(Words.*), " "), "target/test/resources/words.txt")
  }

  val sandbox = new Sandbox("target/test/output")

  "simple query" should {
    "map single field" in {
      val result = sandbox run {
        for (az <- AtoZ) yield az.letter
      }
      result.size should be === 26
      result should contain (new Tuple("a"))
      result should contain (new Tuple("z"))
    }

    "map multiple fields" in {
      val result = sandbox run {
        for (az <- AtoZ) yield az.letter ~ az.number
      }

      result.size should be === 26
      result should contain (new Tuple("a", "1"))
      result should contain (new Tuple("z", "26"))
    }

    "filter tuples using where and ===" in {
      val result = sandbox run {
        for {
          az <- AtoZ where (_.letter === "a")
        } yield az.letter ~ az.number
      }
      result should be === Seq(new Tuple("a", "1"))
    }

    "filter tuples using if and ===" in {
      val result = sandbox run {
        for {
          az <- AtoZ if (az.letter === "a")
        } yield az.letter ~ az.number
      }
      result should be === Seq(
        new Tuple("a", "1")
      )
    }

    "concatenate fields and coerse field to string" in {
      val result = sandbox run {
        for {
          concat <- AtoZ.letter ++ AtoZ.number.asColumnOf[String]
        } yield concat
      }
      result.size should be === 26
      result should contain (new Tuple("a1"))
      result should contain (new Tuple("b2"))
      result should contain (new Tuple("c3"))
    }

    "filter tuples with a set" in {
      val result = sandbox run {
        for {
          abc <- AtoZ where (_.letter in Set("a", "b", "c"))
        } yield abc
      }

      result.size should be === 3
      result should contain (new Tuple("a", "1"))
      result should contain (new Tuple("b", "2"))
      result should contain (new Tuple("c", "3"))
    }

    "apply multiple conditions" in {
      val result = sandbox run {
        for {
          _ <- AtoZ where (_.letter in Set("a", "b"))
          _ <- AtoZ where (_.letter in Set("a"))
        } yield AtoZ
      }
      result.size should be === 1
      result should contain (new Tuple("a", "1"))
    }

    "filter using multiple conditions with logical AND" in {
      val result = sandbox run {
        for {
          _ <- AtoZ where { az => az.number > 5 && az.number <= 7 }
        } yield AtoZ
      }

      result.size should be === 2
      result should contain (new Tuple("f", "6"))
      result should contain (new Tuple("g", "7"))
    }

    "join tables using innerJoin" in {
      val result = sandbox run {
        for {
          Join(az, words) <- (AtoZ.innerJoin(Words)) on (_.letter is _.letter)
        } yield az.letter ~ az.number ~ words.word
        /*
        AtoZ innerJoin Words on (_.letter is _.letter) map { case Join(az, words) =>
          az.letter ~ az.number ~ words.word
        }
        */
      }
      result.size should be === 8
      result should contain (new Tuple("a", "1", "apple"))
      result should contain (new Tuple("h", "8", "house"))
    }

    "join previous query using innerJoin" in {
      val result = sandbox run {
        val lastLetter = new NamedProjection(
          for {
            w <- Words
            last <- Words.letter mapValue (_.last.toString)
          } yield new Projection1(last)
        ) {
          val last = *._1
        }

        for {
          Join(lastLetter, words) <- (lastLetter innerJoin Words) on (_.last is _.letter)
        } yield lastLetter.last ~ words.word
      }
      result.size should be === 8
      result should contain (new Tuple("a", "apple"))
      result should contain (new Tuple("h", "house"))
    }

    "join 3 tables" in {
      val result = sandbox run {
        val lastLetter = new NamedProjection(
          for {
            w <- Words
            last <- Words.letter mapValue (_.last.toString)
          } yield new Projection1(last)
        ) {
          val last = *._1
        }

        for {
          Join(lastLetter, words) <- (lastLetter innerJoin Words) on (_.last is _.letter)
          Join(lastLetter, az)    <- (lastLetter innerJoin AtoZ)  on (_.last is _.letter)
        } yield lastLetter.last ~ az.number ~ words.word
      }
      result.size should be === 8
      result should contain (new Tuple("a", "1", "apple"))
      result should contain (new Tuple("h", "8", "house"))
    }

    "allow mapping values using an inlined closure" in {
      val result = sandbox run {
        val query = for {
          abc <- AtoZ
          vowel <- abc.letter mapValue { letter => if ("aeiouy" contains letter) "yes" else "no" }
        } yield abc.letter ~ vowel
        query
      }
      result.size should be === 26
      result should contain (new Tuple("a", "yes"))
      result should contain (new Tuple("b", "no"))
      result.filter(_.get(1) == "yes").size should be === 6
    }

    "allow mapping + filtering using mapOption" in {
      val result = sandbox run {
        val query = for {
          abc <- AtoZ
          vowel <- abc.letter mapOption { letter => if ("aeiouy" contains letter) Some("yes") else None }
        } yield abc.letter ~ vowel
        query
      }
      result.size should be === 6
      result should contain (new Tuple("a", "yes"))
      result should contain (new Tuple("e", "yes"))
      result should contain (new Tuple("i", "yes"))
      result should contain (new Tuple("o", "yes"))
      result should contain (new Tuple("u", "yes"))
      result should contain (new Tuple("y", "yes"))
    }

    "allow mapping + filtering using mapPartial" in {
      val result = sandbox run {
        val query = for {
          abc <- AtoZ
          vowel <- abc.letter mapPartial { case l if ("aeiouy" contains l) => "yes" }
        } yield abc.letter ~ vowel
        query
      }
      result.size should be === 6
      result should contain (new Tuple("a", "yes"))
      result should contain (new Tuple("e", "yes"))
      result should contain (new Tuple("i", "yes"))
      result should contain (new Tuple("o", "yes"))
      result should contain (new Tuple("u", "yes"))
      result should contain (new Tuple("y", "yes"))
    }

    "support one-to-many mapping + filtering using mapValues" in {
      def explode(n: Int) = Seq.tabulate(n) { x => "%d-%d" format (n, x+1) }

      val result = sandbox run {
        val query = for {
          abc <- AtoZ
          exploded <- abc.number mapValues { n => if (n > 2 && n < 5) explode(n) else Seq.empty }
        } yield abc.letter ~ exploded
        query
      }
      result.size should be === 7
      result should contain (new Tuple("c", "3-1"))
      result should contain (new Tuple("c", "3-2"))
      result should contain (new Tuple("c", "3-3"))
      result should contain (new Tuple("d", "4-1"))
      result should contain (new Tuple("d", "4-2"))
      result should contain (new Tuple("d", "4-3"))
      result should contain (new Tuple("d", "4-4"))
    }

    "join table with intermediate query" in {
      val result = sandbox run {
        val complement = new NamedProjection(
          for {
            az <- AtoZ
            complement <- az.number mapValue (27 - _)
          } yield az.letter ~ complement
        ) {
          val letter ~ complement = *
        }

        for {
          Join(az, c) <- (AtoZ innerJoin complement) on (_.letter is _.letter)
        } yield az.letter ~ az.number ~ c.complement
      }
      result.size should be === 26
      result should contain (new Tuple("a", "1", "26"))
      result should contain (new Tuple("z", "26", "1"))
    }

  }
}
