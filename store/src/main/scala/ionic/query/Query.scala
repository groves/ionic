package ionic.query

object Query {
  def parse(query: String) = new IQLParser().parse(query)
}
case class Query(val from: String, val where: Where)

case class Where(val clauses: Clause*)

abstract class Clause(val f: String)

case class LongCond(override val f: String, value: Long, pred: (Long, Long) => Boolean) extends Clause(f) {
  def meets(other: Long): Boolean = pred(other, value)
}

case class BooleanEquals(override val f: String, val value: Boolean) extends Clause(f)
case class StringEquals(override val f: String, val value: String) extends Clause(f)
