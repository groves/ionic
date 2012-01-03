package ionic.store.series

import scala.collection.JavaConversions._

import org.apache.avro.Schema
import org.apache.avro.io.Decoder

import com.threerings.fisy.Directory

class SplitSeriesWriter(schema: Schema, base: Directory) {

  val dest = base.navigate(SeriesWriter.genDir(Series.splitPrefix, schema.getFullName()))

  private val writers = schema.getFields.map(f =>
    if (f.schema.getType == Schema.Type.LONG && f.name == "timestamp") {
      new SortedLongColumnWriter(dest, f)
    } else {
      new PassthroughAvroColumnWriter(dest, f)
    })

  private var written = 0
  private var closed = false

  SeriesWriter.writeSchema(schema, dest)
  def write(decoder: Decoder) {
    writers.foreach(_.write(decoder))
    written += 1
  }
  def close() {
    if (closed) { return }
    closed = true
    writers.foreach(_.close())
    SeriesWriter.writeMeta(written, dest)
  }
}
