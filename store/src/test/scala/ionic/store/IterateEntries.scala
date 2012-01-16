package ionic.store

import scala.collection.JavaConversions._

import ionic.store.series.ReadSimpleColumns
import ionic.store.series.WriteSimpleColumns

import org.apache.avro.Schema.Type._

import org.scalatest.FunSuite

import com.threerings.fisy.Directory
import com.threerings.fisy.Paths

object IterateEntries {
  val schema =
    WriteSimpleColumns.makeSchema(("timestamp", LONG), ("playerId", LONG), ("score", FLOAT))

  def writeSeries(fs: Directory, baseTs: Long) {
    ReadSimpleColumns.writeToFs(fs, schema, 3, (encoder) => {
      encoder.writeLong(baseTs)
      encoder.writeLong(2)
      encoder.writeFloat(.75F)
      encoder.writeLong(baseTs)
      encoder.writeLong(1)
      encoder.writeFloat(.25F)
      encoder.writeLong(baseTs + 540)
      encoder.writeLong(2)
      encoder.writeFloat(.75F)
    })
  }

}
class IterateEntries extends FunSuite {
  test("read nothing") {
    assert(EntryReader("blah", Paths.makeTempFs()).size == 0)
  }

  test("read one series") {
    val fs = Paths.makeTempFs()
    IterateEntries.writeSeries(fs, 1234)
    assert(3 === EntryReader(IterateEntries.schema.getFullName(), fs).size)
  }

  test("read two series") {
    val fs = Paths.makeTempFs()
    IterateEntries.writeSeries(fs, 1234)
    IterateEntries.writeSeries(fs, 5678)
    val entries = EntryReader(IterateEntries.schema.getFullName(), fs).toList
    assert(entries.count(_.get("timestamp") == 1234) === 2)
    assert(entries.count(_.get("timestamp") == 5678) === 2)
    assert(3.5 === entries.map(_.get("score").asInstanceOf[Float]).sum) // Bleh
    assert(6 === entries.size)
  }
}
