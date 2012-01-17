package ionic.store.series

import org.apache.avro.io.Decoder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.util.Utf8
import org.apache.avro.Schema
import com.threerings.fisy.Directory
import org.xerial.snappy.SnappyOutputStream

class SnappyColumnWriter (dest: Directory, field: Schema.Field) extends ColumnWriter {
  private val out = new SnappyOutputStream(dest.open(field.name).write())
  private var utf8Buf = new Utf8
  private val encoder = EncoderFactory.get().directBinaryEncoder(out, null)

  def write(decoder :Decoder) {
    utf8Buf = decoder.readString(utf8Buf)
    encoder.writeString(utf8Buf)
  }
  def close() { out.close() }
}
