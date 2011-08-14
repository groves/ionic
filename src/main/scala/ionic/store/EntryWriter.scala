package ionic.store

import java.io.OutputStreamWriter

import scala.collection.JavaConversions._

import com.google.common.base.Charsets

import org.apache.avro.Schema
import org.apache.avro.io.Decoder

import com.threerings.fisy.Directory

class EntryWriter(schema: Schema, dest: Directory) {
  private val writers = schema.getFields.map(f =>
    if (f.schema.getType == Schema.Type.LONG && f.name == "timestamp") {
      new SortedLongColumnWriter(dest, f)
    } else {
      new PassthroughAvroColumnWriter(dest, f)
    })

  new OutputStreamWriter(dest.open("schema.avsc").write(), Charsets.UTF_8).
    append(schema.toString(true)).
    close()

  def write(decoder: Decoder) { writers.foreach(_.write(decoder)) }
  def close() { writers.foreach(_.close()) }
}
