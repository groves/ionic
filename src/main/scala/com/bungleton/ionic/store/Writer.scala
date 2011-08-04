package com.bungleton.ionic.store

import org.apache.avro.Schema
import org.apache.avro.io.Decoder
import java.io.InputStream
import com.threerings.fisy.Directory
import scala.collection.JavaConversions._

class Writer (schema :Schema, dest :Directory) {
  private val writers = schema.getFields.map(f =>
      if (f.schema.getType == Schema.Type.LONG && f.name == "timestamp") {
        new SortedLongColumn(dest, f)
      } else {
        new PassthroughAvroColumn(dest, f)
      }
    )

  def write(decoder :Decoder) { writers.foreach(_.write(decoder)) }
  def close() { writers.foreach(_.close()) }
}
