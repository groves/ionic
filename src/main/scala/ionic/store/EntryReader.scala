package ionic.store

import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.Decoder
import org.apache.avro.Schema
import com.threerings.fisy.Directory
import scala.collection.JavaConversions._

class EntryReader(source: Directory) {
  private val schema = Schema.parse(source.open("schema.avsc").read())
  private val readers = schema.getFields.map(f =>
    if (f.schema.getType == Schema.Type.LONG && f.name == "timestamp") {
      new SortedLongColumnReader(source, f)
    } else {
      new PassthroughAvroColumnReader(source, f)
    })

  def read(old: GenericRecord = null): GenericRecord = {
    val record = if (old != null) { old } else { new GenericData.Record(schema) }
    readers.foreach(_.read(record))
    record
  }
  def close() { readers.foreach(_.close()) }
}
