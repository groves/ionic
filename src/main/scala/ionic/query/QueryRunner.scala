package ionic.query

class QueryRunner(query: String) {
  val parsed: Query = new IQLParser().parse(query)
}
