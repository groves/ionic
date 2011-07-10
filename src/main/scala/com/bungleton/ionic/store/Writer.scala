package com.bungleton.ionic.store

import org.apache.avro.Schema
import org.apache.avro.io.Decoder
import java.io.InputStream
import com.threerings.fisy.Directory
import scala.collection.JavaConversions._

class Writer (schema :Schema, decoder :Decoder, dest :Directory) {
  private val writers = schema.getFields.map(f => new PassthroughAvroColumn(decoder, dest, f))

  def write() { writers.foreach(_.write) }
}
