package ionic.store.series

import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.io.Decoder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.util.Utf8

import com.threerings.fisy.Directory

class PassthroughAvroColumnWriter(dest: Directory, field: Schema.Field)
  extends ColumnWriter {
  private var utf8Buf = new Utf8
  private var byteBuf: ByteBuffer = null
  private val out = dest.open(field.name).write()
  private val encoder = EncoderFactory.get().directBinaryEncoder(out, null)
  private val writer = field.schema.getType match {
    case NULL => (_: Decoder) => ()
    case BOOLEAN => (decoder: Decoder) => encoder.writeBoolean(decoder.readBoolean())
    case INT => (decoder: Decoder) => encoder.writeInt(decoder.readInt())
    case LONG => (decoder: Decoder) => encoder.writeLong(decoder.readLong())
    case FLOAT => (decoder: Decoder) => encoder.writeFloat(decoder.readFloat())
    case DOUBLE => (decoder: Decoder) => encoder.writeDouble(decoder.readDouble())
    case ENUM => (decoder: Decoder) => encoder.writeEnum(decoder.readEnum())
    case STRING => (decoder: Decoder) => {
      utf8Buf = decoder.readString(utf8Buf)
      encoder.writeString(utf8Buf)
    }
    case BYTES => (decoder: Decoder) => {
      byteBuf = decoder.readBytes(byteBuf)
      encoder.writeBytes(byteBuf)
    }
    case x => throw new IllegalArgumentException("Unknown schema type: " + x)
  }
  def write(decoder: Decoder) { writer(decoder) }
  def close() { out.close() }
}
