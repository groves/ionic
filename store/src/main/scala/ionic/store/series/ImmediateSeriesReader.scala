package ionic.store.series

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader

import com.threerings.fisy.Directory

class ImmediateSeriesReader(source: Directory)
  extends Iterator[GenericRecord] {
  private val schema = Schema.parse(source.open("schema.avsc").read())
  private val metaDecoder =
    DecoderFactory.get().jsonDecoder(SeriesMetadata.SCHEMA$, source.open("meta.avsc").read())
  private val metaReader: DatumReader[SeriesMetadata] = new SpecificDatumReader(SeriesMetadata.SCHEMA$)
  private val meta = metaReader.read(new SeriesMetadata(), metaDecoder)
  private val recordDecoder =
    DecoderFactory.get().binaryDecoder(source.open("series").read(), null)
  private val recordReader = new GenericDatumReader[GenericRecord](schema)
  private var _read = 0

  def hasNext(): Boolean = { _read != meta.entries }
  def next(): GenericRecord = {
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
