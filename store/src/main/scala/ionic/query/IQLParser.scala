package ionic.query

import scala.util.parsing.combinator.JavaTokenParsers

class IQLParser extends JavaTokenParsers {

  def query: Parser[Query] = qident ~ opt(where) ^^ {
    case qident ~ None => Query(qident, Where())
    case qident ~ Some(clauses) => Query(qident, clauses)
  }

  def qident: Parser[String] = "[\\w.]+".r

  def where: Parser[Where] = "where" ~> repsep(predicate, "and") ^^ (Where(_: _*))

  def predicate = (ident ~ "=" ~ boolean ^^ { case f ~ _ ~ b => BooleanEquals(f, b) }
    | ident ~ "=" ~ stringLiteral ^^ { case f ~ _ ~ v => StringEquals(f, stripQuotes(v)) }
    | ident ~ "([!><]?=|<|>)".r ~ decimalNumber ^^ { case f ~ p ~ d => NumCond(f, d, p) })

  def boolean = ("true" ^^^ (true) | "false" ^^^ (false))

  def stripQuotes(s: String) = s.substring(1, s.length - 1)

  def parse(iql: String): Query = parseAll(query, iql) match {
    case Success(r: Query, _) => r
    case x => throw new RuntimeException("Parse failed: " + x)
  }
}
