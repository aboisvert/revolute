package revolute.examples

import revolute._
import revolute.query._

import BasicImplicitConversions._

object Persons extends Table[(String, Int)]("persons") {
  def name = column[String]("id")
  def age = column[Int]("first")
  def * = name ~ age
}

object Follows extends Table[(String, String)]("follows") {
  def follower = column[String]("follower")
  def followee = column[String]("followee")
  def * = follower ~ followee
}

object ExampleQueries {
  /* List all users whose last name is "Boisvert" */
  for (p <- Persons where (_.name matches "Boisvert$")) yield p.name

  /* Predator detection */
  /* TODO: Need numeric expressions
  for {
    p1 <- Persons
    p2 <- Persons where { p => p.age < 18 && p1.age - p.age > 5 }
    _ <- Follows where { f => (f.follower is p1.name) && (f.followee is p2.name) }
    _ <- Query.orderBy((p1.age - p2.age) desc)
  } yield p1.name ~ p2.name
  */
  
  /* How many people do people follow? */
  val x = for {
    p <- Persons
    f <- Follows where (_.follower is p.name)
  } yield p.name ~ f.count
  println(x)
}