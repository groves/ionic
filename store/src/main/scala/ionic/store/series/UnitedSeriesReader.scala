package ionic.store.series

import ionic.query.Where

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory

import com.threerings.fisy.Directory

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
  private var _read = 0L

  override def hasNext(): Boolean = _read != meta.entries
  override def next(): GenericRecord = {
    assert(hasNext())
    read()
  }
  def read(old: GenericRecord = null): GenericRecord = {
    _read += 1
    val value = recordReader.read(old, recordDecoder)
    if (_read == meta.entries) close()
    value
  }
  def close() {
    _read = meta.entries
    recordDecoder.inputStream.close()
  }

}
