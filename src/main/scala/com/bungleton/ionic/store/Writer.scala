package com.bungleton.ionic.store

import org.apache.avro.Schema
import org.apache.avro.io.Decoder
import java.io.InputStream
import com.threerings.fisy.Directory
import scala.collection.JavaConversions._

class Writer (val schema :Schema, val decoder :Decoder, val dest :Directory) {
  private val writers = schema.getFields.map(f => new BooleanAvroColumn(decoder, dest, f.name))

  def write() { writers.foreach(_.write) }
}
