package ionic.query

case class Query(val columns: List[String], val from: String, val where: Option[Where])

case class Where(val clauses: Clause*)

abstract class Clause {
  def and(otherField: Clause): Clause = And(this, otherField)
  def or(otherField: Clause): Clause = Or(this, otherField)
}

case class StringEquals(val f: String, val value: String) extends Clause
case class NumberEquals(val f: String, val value: Number) extends Clause
case class BooleanEquals(val f: String, val value: Boolean) extends Clause
case class In(val field: String, val values: String*) extends Clause
case class And(val lClause: Clause, val rClause: Clause) extends Clause
case class Or(val lClause: Clause, val rClause: Clause) extends Clause
