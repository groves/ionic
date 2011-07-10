package com.bungleton.ionic.store

import org.apache.avro.io.EncoderFactory
import com.threerings.fisy.Directory
import org.apache.avro.io.Decoder
import org.apache.avro.Schema

class PassthroughAvroColumn (decoder :Decoder, dest :Directory, field :Schema.Field) {
  private val out = dest.open(field.name).write()
  private val encoder = EncoderFactory.get().directBinaryEncoder(out, null)
  private val writer = field.schema.getType match {
    case Schema.Type.BOOLEAN => () => encoder.writeBoolean(decoder.readBoolean())
    case Schema.Type.INT => () => encoder.writeInt(decoder.readInt())
    case x => throw new IllegalArgumentException("Unknown schema type: " + x)
  }
  def write() { writer() }
}
