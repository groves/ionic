package com.bungleton.ionic.store

import org.apache.avro.io.DecoderFactory
import org.apache.avro.generic.IndexedRecord
import java.nio.ByteBuffer
import org.apache.avro.util.Utf8
import org.apache.avro.io.EncoderFactory
import com.threerings.fisy.Directory
import org.apache.avro.io.Decoder
import org.apache.avro.Schema

import org.apache.avro.Schema.Type._

class PassthroughAvroColumnReader (source :Directory, field :Schema.Field) {
  private var utf8Buf = new Utf8
  private var byteBuf :ByteBuffer = null
  private val in = source.open(field.name).read()
  private val decoder = DecoderFactory.get().binaryDecoder(in, null)
  private val reader = field.schema.getType match {
    case NULL => (_ :IndexedRecord) => ()
    case BOOLEAN => (rec :IndexedRecord) => rec.put(field.pos, decoder.readBoolean())
/*    case INT => (rec :IndexedRecord) => encoder.writeInt(decoder.readInt())
    case LONG => (rec :IndexedRecord) => encoder.writeLong(decoder.readLong())
    case FLOAT => (rec :IndexedRecord) => encoder.writeFloat(decoder.readFloat())
    case DOUBLE => (rec :IndexedRecord) => encoder.writeDouble(decoder.readDouble())
    case STRING => (rec :IndexedRecord) => {
      utf8Buf = decoder.readString(utf8Buf)
      encoder.writeString(utf8Buf)
    }
    case BYTES => (rec :IndexedRecord) => {
      byteBuf = decoder.readBytes(byteBuf)
      encoder.writeBytes(byteBuf)
    }*/
    case x => throw new IllegalArgumentException("Unknown schema type: " + x)
  }
  def read(rec :IndexedRecord) { reader(rec) }
  def close() { in.close() }
}
