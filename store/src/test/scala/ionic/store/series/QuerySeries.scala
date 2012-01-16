package ionic.store.series

import com.codahale.logula.Logging

import ionic.query.Query

import org.apache.log4j.Level

import org.scalatest.FunSuite

class QuerySeries extends FunSuite {
  import ParcelOps._
  Logging.configure { log => log.level = Level.INFO }

  test("gt on united writer") {
    val parceler = makeParceler
    write(parceler, (1234L, 1L, 12.7F), (1234L, 2L, 12.7F))
    assert(parceler.reader().size === 2)
    assert(parceler.reader(Query.parse("ionic.Simple where playerId > 1")).size === 1)
  }

  test("dual condition on mixed writers") {
    val parceler = makeParceler
    write(parceler, (1234L, 1L, 12.7F), (2234L, 2L, 18.7F)).close()
    write(parceler, (1234L, 1L, 19.7F), (2234L, 2L, 4F))
    waitForSplit(parceler)
    assert(parceler.reader().size === 4)
    assert(parceler.reader(Query.parse("ionic.Simple where playerId = 2 and score > 15.02")).size === 1)
  }
}
