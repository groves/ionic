package ionic.store.series

import scala.collection.JavaConversions._

import org.apache.avro.Schema
import org.apache.avro.io.Decoder

import com.threerings.fisy.Directory

object SplitSeriesWriter {
  def transferFrom(base: Directory, writer: UnitedSeriesWriter): SplitSeriesWriter =
    transferFrom(base, writer.schema, writer.dest, writer.written)
  def transferFrom(base: Directory, schema: Schema, source: Directory, entries: Int) = {
    val split = new SplitSeriesWriter(schema, base, source.getPath)
    val decoder = UnitedSeriesReader.makeDecoder(source)
    (1 to entries).foreach(_ => { split.write(decoder) })
    split.close()
    split
  }
}

class SplitSeriesWriter(schema: Schema, base: Directory, transferFrom: String = "") {
  val dest = base.navigate(SeriesWriter.genDir(Series.splitPrefix, schema.getFullName()))

  private val writers = schema.getFields.map(f =>
    if (f.schema.getType == Schema.Type.LONG && f.name == "timestamp") {
      new SortedLongColumnWriter(dest, f)
    } else {
      new PassthroughAvroColumnWriter(dest, f)
    })

  private var _written = 0
  def written = _written

  private var closed = false

  SeriesWriter.writeSchema(schema, dest)
  SeriesWriter.writeMeta(dest, transferredFrom = transferFrom)
  def write(decoder: Decoder) {
    writers.foreach(_.write(decoder))
    _written += 1
  }
  def close() {
    if (closed) { return }
    closed = true
    writers.foreach(_.close())
    SeriesWriter.writeMeta(dest, written, transferredFrom = transferFrom)
  }
}
