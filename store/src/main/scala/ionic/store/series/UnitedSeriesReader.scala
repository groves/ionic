package ionic.store.series

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory

import com.threerings.fisy.Directory

import ionic.query.Where

object UnitedSeriesReader {
  val prefix = "united"

  def dir(schema: Schema): String = dir(schema.getFullName)
  def dir(name: String): String = prefix + "/" + name
}

class UnitedSeriesReader(source: Directory, where: Where)
  extends Iterator[GenericRecord] {
  private val schema = SeriesReader.readSchema(source)
  private val meta = SeriesReader.readMeta(source)
  private val recordDecoder =
    DecoderFactory.get().binaryDecoder(source.open("series").read(), null)
  private val recordReader = new GenericDatumReader[GenericRecord](schema)
  private var _read = 0

  override def hasNext(): Boolean = _read != meta.entries
  override def next(): GenericRecord = {
    assert(hasNext())
    read()
  }
  def read(old: GenericRecord = null): GenericRecord = {
    _read += 1
    recordReader.read(old, recordDecoder)
  }
  def close() {
    _read = meta.entries.asInstanceOf[Int]
    recordDecoder.inputStream.close()
  }

}
