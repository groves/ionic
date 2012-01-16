package ionic.query

import scala.collection.JavaConversions._

import ionic.store.IterateEntries
import ionic.store.series.SeriesParceler

import org.apache.avro.generic.GenericRecord

import org.scalatest.FunSuite

import com.threerings.fisy.Paths

class RunQuery extends FunSuite {
  def run(query: String) = {
    val fs = Paths.makeTempFs()
    IterateEntries.writeSeries(fs, 1234)
    new SeriesParceler(fs, "ionic.Simple").reader(Query.parse(query))
  }
  test("extract all") { assert(3 === run("ionic.Simple").size) }
  test("extract of value") {
    assert(1 === run("ionic.Simple where playerId = 1").size)
    var rec: GenericRecord = run("ionic.Simple where playerId = 1").head
    assert(.25 === rec.get("score"))
  }
  test("extract not of value") { assert(1 === run("ionic.Simple where playerId < 2").size) }
  test("extract greater than value") {
    assert(1 === run("ionic.Simple where timestamp > 1234").size)
  }
  test("extract le than value") { assert(2 === run("ionic.Simple where timestamp <= 1234").size) }
  test("extract double gt") { assert(2 === run("ionic.Simple where score > .25").size) }
  test("extract with and") { assert(1 === run("ionic.Simple where timestamp <= 1234 and playerId > 1").size) }
  test("extract and excludes") { assert(0 === run("ionic.Simple where timestamp <= 1234 and playerId > 3").size) }
}
