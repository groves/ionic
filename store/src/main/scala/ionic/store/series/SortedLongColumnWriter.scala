package ionic.store.series

import org.apache.avro.Schema
import org.apache.avro.io.Decoder
import org.apache.avro.io.EncoderFactory

import com.threerings.fisy.Directory

class SortedLongColumnWriter(dest: Directory, field: Schema.Field) extends ColumnWriter {
  private val out = dest.open(field.name).write()
  private val encoder = EncoderFactory.get().directBinaryEncoder(out, null)

  private var previous: Long = 0
  private var offset: Long = 0
  private var countAtPrevious: Int = 0
  private var closed: Boolean = false

  def write(decoder: Decoder) {
    assert(!closed)
    val current = decoder.readLong()

    if (current != previous) {
      if (countAtPrevious > 0) {
        encoder.writeLong(offset)
        encoder.writeInt(countAtPrevious)
      }
      offset = current - previous
      previous = current
      countAtPrevious = 1
    } else {
      countAtPrevious += 1
    }
  }

  def close() {
    if (countAtPrevious > 0) {
      encoder.writeLong(offset)
      encoder.writeInt(countAtPrevious)
    }
    closed = true
    out.close()
  }
}
