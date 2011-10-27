package ionic.store.series

import scala.collection.JavaConversions._

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

import com.threerings.fisy.Directory

object SplitSeriesReader {
  val prefix = "split"

  def dir(schema: Schema): String = dir(schema.getFullName)
  def dir(name: String): String = prefix + "/" + name
}

class SplitSeriesReader(source: Directory)
  extends Iterator[GenericRecord] {
  private val schema = SeriesReader.readSchema(source)
  private val readers = schema.getFields.map(f =>
    if (f.schema.getType == Schema.Type.LONG && f.name == "timestamp") {
      new SortedLongColumnReader(source, f)
    } else {
      new PassthroughAvroColumnReader(source, f)
    })
  private var _read = 0

  private val meta = SeriesReader.readMeta(source)
  def hasNext(): Boolean = { _read != meta.entries }
  def next(): GenericRecord = {
    assert(hasNext())
    read()
  }
  def read(old: GenericRecord = null): GenericRecord = {
    _read += 1
    val record = if (old != null) { old } else { new GenericData.Record(schema) }
    readers.foreach(_.read(record))
    record
  }
  def close() { readers.foreach(_.close()) }
}
