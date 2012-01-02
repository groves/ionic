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
  test("read closed write") {
    val fs = Paths.makeTempFs()
    val parceler = new SeriesParceler(fs, "ionic.Simple")
    val schema =
      WriteSimpleColumns.makeSchema(List(("timestamp", LONG), ("playerId", LONG), ("score", FLOAT)))
    val writer = parceler.writer(schema)
    val record = new GenericData.Record(schema)
    record.put("timestamp", 1234L)
    record.put("playerId", 1L)
    record.put("score", 12.7F)
    val out = new ByteBufferOutputStream()
    val enc = EncoderFactory.get.directBinaryEncoder(out, null)
    new SpecificDatumWriter(record.getSchema).write(record, enc)
    out.getBufferList().foreach(writer.write(_))
    writer.close()
    assert(parceler.reader().size === 1)
  }
}
