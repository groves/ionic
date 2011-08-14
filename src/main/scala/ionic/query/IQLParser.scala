package ionic.query

import scala.util.parsing.combinator.JavaTokenParsers

class IQLParser extends JavaTokenParsers {

  def query: Parser[Query] = columns ~ from ~ opt(where) ^^ {
    case columns ~ from ~ where => Query(columns, from, where)
  }

  def columns: Parser[List[String]] = repsep(ident, ",")

  def from: Parser[String] = "from" ~> ident

  def where: Parser[Where] = "where" ~> rep(clause) ^^ (Where(_: _*))

  def clause: Parser[Clause] = (predicate | parens) * ("and" ^^^ { (a: Clause, b: Clause) => And(a, b) } |
    "or" ^^^ { (a: Clause, b: Clause) => Or(a, b) })

  def parens: Parser[Clause] = "(" ~> clause <~ ")"

  def predicate = (ident ~ "=" ~ boolean ^^ { case f ~ _ ~ b => BooleanEquals(f, b) }
    | ident ~ "=" ~ stringLiteral ^^ { case f ~ _ ~ v => StringEquals(f, stripQuotes(v)) }
    | ident ~ "=" ~ wholeNumber ^^ { case f ~ _ ~ i => NumberEquals(f, i.toInt) })

  def boolean = ("true" ^^^ (true) | "false" ^^^ (false))

  def stripQuotes(s: String) = s.substring(1, s.length - 1)

  def parse(iql: String): Query = parseAll(query, iql) match {
    case Success(r: Query, _) => r
    case x => throw new RuntimeException("Parse failed: " + x)
  }
}
