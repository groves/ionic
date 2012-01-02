package ionic.store.series

import scala.collection.JavaConversions._

import org.apache.avro.io.EncoderFactory
import org.apache.avro.util.ByteBufferOutputStream
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.generic.GenericData
import com.threerings.fisy.Paths
import org.scalatest.FunSuite
import org.apache.avro.Schema.Type._

class ParcelSeries extends FunSuite {
  val schema =
    WriteSimpleColumns.makeSchema(List(("timestamp", LONG), ("playerId", LONG), ("score", FLOAT)))

  def makeParceler = new SeriesParceler(Paths.makeTempFs(), schema.getFullName)

  def makeRecord(ts: Long, playerId: Long, score: Float) = {
    val record = new GenericData.Record(schema)
    record.put("timestamp", ts)
    record.put("playerId", playerId)
    record.put("score", score)
    record
  }

  def write(parceler: SeriesParceler, values: Tuple3[Long, Long, Float]*): UnitedSeriesWriter =
    write(parceler.writer(schema), values: _*)

  def write(writer: UnitedSeriesWriter, values: Tuple3[Long, Long, Float]*): UnitedSeriesWriter = {
    val out = new ByteBufferOutputStream()
    val enc = EncoderFactory.get.directBinaryEncoder(out, null)
    val datumWriter = new SpecificDatumWriter[GenericData.Record](schema)
    values.map((makeRecord _).tupled(_)).foreach((r: GenericData.Record) => {
      datumWriter.write(r, enc)
      out.getBufferList().foreach(writer.write(_))
    })
    writer
  }

  test("read from closed writer") {
    val parceler = makeParceler
    write(parceler, (1234L, 1L, 12.7F)).close()
    assert(parceler.reader().size === 1)
  }

  test("read from open writer") {
    val parceler = makeParceler
    write(parceler, (1234L, 1L, 12.7F), (2345L, 1L, 17.6F))
    assert(parceler.reader().size === 2)
  }

  test("multiple writers in various states") {
    val parceler = makeParceler
    val openWriter = write(parceler, (1234L, 1L, 12.7F), (1235L, 2L, 10.5F), (1235L, 1L, 5F))
    write(parceler, (2234L, 1L, 12.7F), (2235L, 2L, 10.5F), (2235L, 1L, 5F)).close()
    val reader = parceler.reader()
    write(openWriter, (4342L, 1L, -10.4F))
    assert(reader.size === 6)
    assert(parceler.reader().size === 7)
    openWriter.close()
    assert(parceler.reader().size === 7)
  }
}
