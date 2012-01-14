package ionic.store.series

import ionic.query.Where

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory

import com.threerings.fisy.Directory

object UnitedSeriesReader {
  def makeDecoder(source: Directory) =
    DecoderFactory.get().binaryDecoder(source.open("series").read(), null)
}
class UnitedSeriesReader(val source: Directory, where: Where = Where(), var entries: Long = -1L)
  extends Iterator[GenericRecord] {
  val schema = SeriesReader.readSchema(source)
  if (entries == -1L) {
    entries = SeriesReader.readMeta(source).entries
  }
  private val recordDecoder = UnitedSeriesReader.makeDecoder(source)
  private val recordReader = new GenericDatumReader[GenericRecord](schema)
  private var _read = 0L

  override def hasNext(): Boolean = _read != entries
  override def next(): GenericRecord = {
    assert(hasNext())
    read()
  }
  def read(old: GenericRecord = null): GenericRecord = {
    _read += 1
    val value = recordReader.read(old, recordDecoder)
    if (_read == entries) close()
    value
  }
  def close() {
    _read = entries
    recordDecoder.inputStream.close()
  }

}
