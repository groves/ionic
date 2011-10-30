package ionic.query

import scala.collection.JavaConversions._

import ionic.store.IterateEntries

import org.scalatest.FunSuite

import com.threerings.fisy.Paths

class RunQuery extends FunSuite {
  test("extract all") {
    val fs = Paths.makeMemoryFs()
    IterateEntries.writeSeries(fs, 1234)
    assert(3 === new QueryRunner("ionic.Simple", fs).size)
  }

  test("extract of value") {
    val fs = Paths.makeMemoryFs()
    IterateEntries.writeSeries(fs, 1234)
    assert(1 === new QueryRunner("ionic.Simple where playerId = 1", fs).size)
  }
}
