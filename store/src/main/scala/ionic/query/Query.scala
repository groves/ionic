package ionic.query

object Query {
  def parse(query: String) = new IQLParser().parse(query)
}
case class Query(val from: String, val where: Where)

case class Where(val clauses: Clause*)

abstract class Clause(val f: String)

trait LongCond {
  def meets(other: Long): Boolean
}

case class StringEquals(override val f: String, val value: String) extends Clause(f)
case class LongEquals(override val f: String, val value: Long) extends Clause(f) with LongCond {
  override def meets(other: Long) = value == other
}
case class BooleanEquals(override val f: String, val value: Boolean) extends Clause(f)
