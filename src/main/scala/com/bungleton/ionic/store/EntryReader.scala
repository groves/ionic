package com.bungleton.ionic.store

import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.Decoder
import org.apache.avro.Schema
import com.threerings.fisy.Directory
import scala.collection.JavaConversions._

class EntryReader (source :Directory) {
  private val schema = Schema.parse(source.open("schema.avsc").read())
  private val readers = schema.getFields.map(f => new PassthroughAvroColumnReader(source, f))

  def read(record :GenericRecord=null) { readers.foreach(_.read(record)) }
  def close() { readers.foreach(_.close()) }
}
