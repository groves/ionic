package ionic.store.series

import java.io.OutputStreamWriter

import scala.collection.JavaConversions._

import com.google.common.base.Charsets

import org.apache.avro.Schema
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.Decoder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter

import com.threerings.fisy.Directory

class SeriesWriter(schema: Schema, dest: Directory) {
  private val writers = schema.getFields.map(f =>
    if (f.schema.getType == Schema.Type.LONG && f.name == "timestamp") {
      new SortedLongColumnWriter(dest, f)
    } else {
      new PassthroughAvroColumnWriter(dest, f)
    })
  private var written = 0
  private var closed = false

  new OutputStreamWriter(dest.open("schema.avsc").write(), Charsets.UTF_8).
    append(schema.toString(true)).
    close()

  def write(decoder: Decoder) {
    writers.foreach(_.write(decoder))
    written += 1
  }
  def close() {
    if (closed) { return }
    closed = true
    writers.foreach(_.close())
    val metaOut = dest.open("meta.avsc").write()
    val encoder = EncoderFactory.get().jsonEncoder(SeriesMetadata.SCHEMA$, metaOut)
    val metaWriter: DatumWriter[SeriesMetadata] = new SpecificDatumWriter(SeriesMetadata.SCHEMA$)
    val meta = new SeriesMetadata()
    meta.entries = written
    metaWriter.write(meta, encoder)
    encoder.flush()
    metaOut.close()
  }
}
