package ionic.store.series

import ionic.query.LongCond

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory

import com.threerings.fisy.Directory

abstract class AvroColumnReader(source: Directory, field: Schema.Field) extends ColumnReader {
  private val in = source.open(field.name).read()
  val decoder = DecoderFactory.get().binaryDecoder(in, null)

  def close() { in.close() }
}

class AvroPrimitiveColumnReader(source: Directory, field: Schema.Field, var entries: Long, reader: AvroPrimitiveReader) extends AvroColumnReader(source, field) {
  def read(rec: IndexedRecord, skip: Long): Option[Long] = {
    (0L until skip).foreach({ _ => reader.skip(decoder) })
    var read = 0L
    var value: Option[Any] = None
    do {
      entries -= 1
      if (entries < 0) return None
      value = reader.read(decoder)
      read += 1
    } while (value == None)
    rec.put(field.pos, value.get)
    Some(read)
  }
}

trait AvroPrimitiveReader {
  def skip(decoder: Decoder) { read(decoder) }
  def read(decoder: Decoder): Option[Any]
}

class AvroLongReader(conds: Iterable[LongCond]) extends AvroPrimitiveReader {
  override def skip(decoder: Decoder) { decoder.readLong() }
  def read(decoder: Decoder): Option[Long] = {
    val value = decoder.readLong()
    if (conds.exists(!_.meets(value))) None
    else Some(value)
  }
}

class BasicAvroPrimitiveReader(reader: ((Decoder) => Option[Any])) extends AvroPrimitiveReader {
  def read(decoder: Decoder) = reader(decoder)
}
