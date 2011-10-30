package ionic.query

import scala.collection.JavaConversions._

import ionic.store.IterateEntries

import org.scalatest.FunSuite

import com.threerings.fisy.Paths

class RunQuery extends FunSuite {
  def count(query: String) = {
    val fs = Paths.makeMemoryFs()
    IterateEntries.writeSeries(fs, 1234)
    new QueryRunner(query, fs).size
  }
  test("extract all") { assert(3 === count("ionic.Simple")) }
  test("extract of value") { assert(1 === count("ionic.Simple where playerId = 1")) }
  test("extract not of value") { assert(1 === count("ionic.Simple where playerId < 2")) }
  test("extract greater than value") { assert(1 === count("ionic.Simple where timestamp > 1234")) }
  test("extract le than value") { assert(2 === count("ionic.Simple where timestamp <= 1234")) }
}
