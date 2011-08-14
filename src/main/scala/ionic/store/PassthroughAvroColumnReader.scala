package ionic.store

import org.apache.avro.io.DecoderFactory
import org.apache.avro.generic.IndexedRecord
import java.nio.ByteBuffer
import org.apache.avro.util.Utf8
import org.apache.avro.io.EncoderFactory
import com.threerings.fisy.Directory
import org.apache.avro.io.Decoder
import org.apache.avro.Schema

import org.apache.avro.Schema.Type._

class PassthroughAvroColumnReader(source: Directory, field: Schema.Field)
  extends ColumnReader {
  private var utf8Buf = new Utf8
  private var byteBuf: ByteBuffer = null
  private val in = source.open(field.name).read()
  private val decoder = DecoderFactory.get().binaryDecoder(in, null)
  private val reader = field.schema.getType match {
    case NULL => () => ()
    case BOOLEAN => () => decoder.readBoolean()
    case INT => () => decoder.readInt()
    case LONG => () => decoder.readLong()
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
  def read(rec: IndexedRecord) { rec.put(field.pos, reader()) }
  def close() { in.close() }
}
