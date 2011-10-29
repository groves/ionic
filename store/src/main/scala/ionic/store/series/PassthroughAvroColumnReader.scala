package ionic.store.series

import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.util.Utf8

import com.threerings.fisy.Directory

class PassthroughAvroColumnReader(source: Directory, field: Schema.Field)
  extends AvroColumnReader(source, field) {
  private var utf8Buf = new Utf8
  private var byteBuf: ByteBuffer = null
  private val reader = field.schema.getType match {
    case NULL => () => ()
    case BOOLEAN => () => decoder.readBoolean()
    case INT => () => decoder.readInt()
    case FLOAT => () => decoder.readFloat()
    case DOUBLE => () => decoder.readDouble()
    case STRING => () => {
      utf8Buf = decoder.readString(utf8Buf)
      utf8Buf
    }
    case BYTES => () => {
      byteBuf = decoder.readBytes(byteBuf)
      byteBuf
    }
    case x => throw new IllegalArgumentException("Unknown schema type: " + x)
  }
  def read(rec: IndexedRecord, skip: Long) = {
    (0L until skip).foreach({ _ => reader() })
    rec.put(field.pos, reader())
    Some(1L)
  }
}
