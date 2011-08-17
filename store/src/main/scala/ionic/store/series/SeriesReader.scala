package ionic.store.series

import java.lang.UnsupportedOperationException
import java.util.Iterator

import scala.collection.JavaConversions._

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader

import com.threerings.fisy.Directory

class SeriesReader(source: Directory)
  extends Iterator[GenericRecord] {
  private val schema = Schema.parse(source.open("schema.avsc").read())
  private val decoder =
    DecoderFactory.get().jsonDecoder(SeriesMetadata.SCHEMA$, source.open("meta.avsc").read())
  private val metaReader: DatumReader[SeriesMetadata] = new SpecificDatumReader(SeriesMetadata.SCHEMA$)
  private val meta = metaReader.read(new SeriesMetadata(), decoder)
  private val readers = schema.getFields.map(f =>
    if (f.schema.getType == Schema.Type.LONG && f.name == "timestamp") {
      new SortedLongColumnReader(source, f)
    } else {
      new PassthroughAvroColumnReader(source, f)
    })
  private var _read = 0

  def hasNext(): Boolean = { _read != meta.entries }
  def next(): GenericRecord = {
    assert(hasNext())
    read()
  }
  def remove() { throw new UnsupportedOperationException("Can't remove from a series") }
  def read(old: GenericRecord = null): GenericRecord = {
    _read += 1
    val record = if (old != null) { old } else { new GenericData.Record(schema) }
    readers.foreach(_.read(record))
    record
  }
  def close() { readers.foreach(_.close()) }
}
