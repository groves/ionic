package com.bungleton.ionic.store

import java.nio.ByteBuffer
import org.apache.avro.util.Utf8
import org.apache.avro.io.EncoderFactory
import com.threerings.fisy.Directory
import org.apache.avro.io.Decoder
import org.apache.avro.Schema

import org.apache.avro.Schema.Type._

object PassthroughAvroColumn {
  val passthroughTypes = Set(NULL, BOOLEAN, INT, LONG, FLOAT, DOUBLE, BYTES, STRING)
}
class PassthroughAvroColumn (decoder :Decoder, dest :Directory, field :Schema.Field)
    extends Column {
  private var utf8Buf = new Utf8
  private var byteBuf :ByteBuffer = null
  private val out = dest.open(field.name).write()
  private val encoder = EncoderFactory.get().directBinaryEncoder(out, null)
  private val writer = field.schema.getType match {
    case NULL => () => ()
    case BOOLEAN => () => encoder.writeBoolean(decoder.readBoolean())
    case INT => () => encoder.writeInt(decoder.readInt())
    case LONG => () => encoder.writeLong(decoder.readLong())
    case FLOAT => () => encoder.writeFloat(decoder.readFloat())
    case DOUBLE => () => encoder.writeDouble(decoder.readDouble())
    case STRING => () => {
      utf8Buf = decoder.readString(utf8Buf)
      encoder.writeString(utf8Buf)
    }
    case BYTES => () => {
      byteBuf = decoder.readBytes(byteBuf)
      encoder.writeBytes(byteBuf)
    }
    case x => throw new IllegalArgumentException("Unknown schema type: " + x)
  }
  def write() { writer() }
  def close() { out.close() }
}
