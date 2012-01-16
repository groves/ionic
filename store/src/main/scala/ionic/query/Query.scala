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

case class DoubleCond(override val f: String, value: Double, pred :(Double, Double) => Boolean) extends Clause(f) {
  def meets(other: Double): Boolean = pred(other, value)
}

case class NumCond(override val f: String, value: String, comp :String) extends Clause(f) {
  def toLong = LongCond(f, value.toLong, comp match {
    case "=" => _ == _
    case "!=" => _ != _
    case ">=" => _ >= _
    case "<=" => _ <= _
    case "<" => _ < _
    case ">" => _ > _
  })
  def toDouble = DoubleCond(f, value.toDouble, comp match {
    case "=" => _ == _
    case "!=" => _ != _
    case ">=" => _ >= _
    case "<=" => _ <= _
    case "<" => _ < _
    case ">" => _ > _
  })
}

case class BooleanEquals(override val f: String, val value: Boolean) extends Clause(f)
case class StringEquals(override val f: String, val value: String) extends Clause(f)
