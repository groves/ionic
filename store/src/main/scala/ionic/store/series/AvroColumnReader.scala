package ionic.store.series

import java.nio.ByteBuffer

import ionic.query.Clause
import ionic.query.DoubleCond
import ionic.query.LongCond
import ionic.query.StringEquals
import ionic.query.NumCond

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.io.Decoder
import org.apache.avro.util.Utf8

class AvroPrimitiveColumnReader(decoder: Decoder, field: Schema.Field, var entries: Long, reader: AvroPrimitiveReader, onClose :() => Unit=()=>{}) extends ColumnReader {

  def close() { onClose() }
  def skip() {
    entries -= 1
    if (entries >= 0) reader.skip(decoder)
  }
  def readOne(rec :IndexedRecord): Boolean = {
    entries -= 1
    if (entries < 0) return false
    reader.read(decoder) match {
      case None => false
      case Some(value) => {
        rec.put(field.pos, value)
        true
      }
    }
  }

  def read(rec: IndexedRecord, skip: Long): Option[Long] = {
    (0L until skip).foreach({ _ => reader.skip(decoder) })
    var read = 0L
    while(entries > 0) {
      read += 1
      if (readOne(rec)) return Some(read)
    }
    None
  }
}

object AvroPrimitiveReader {
  def apply(s: Schema, clauses: Iterable[Clause]) = {
    if (s.getType == STRING) {
      new StringAvroPrimitiveReader()
    } else if (s.getType == BYTES) {
      new BytesAvroPrimitiveReader()
    } else if (s.getType == DOUBLE) {
      new AvroDoubleReader(clauses.collect({ case d: NumCond => d }).map(_.toDouble))
    } else if (s.getType == FLOAT) {
      new AvroFloatReader(clauses.collect({ case d: NumCond => d }).map(_.toDouble))
    } else if (s.getType == LONG) {
      new AvroLongReader(clauses.collect({ case n: NumCond => n }).map(_.toLong))
    } else if (s.getType == ENUM) {
      new AvroEnumReader(s, clauses.collect({ case s: StringEquals => s }))
    } else {
      val decoder = s.getType match {
        case BOOLEAN => (decoder: Decoder) => Some(decoder.readBoolean())
        case INT => (decoder: Decoder) => Some(decoder.readInt())
        case FLOAT => (decoder: Decoder) => Some(decoder.readFloat())
        case DOUBLE => (decoder: Decoder) => Some(decoder.readDouble())
      }
      new BasicAvroPrimitiveReader(decoder)
    }
  }
}

trait AvroPrimitiveReader {
  def skip(decoder: Decoder) { read(decoder) }
  def read(decoder: Decoder): Option[Any]
}

class AvroEnumReader(s :Schema, conds: Iterable[StringEquals]) extends AvroPrimitiveReader {
  val accepted :Set[Int] = conds.map((se :StringEquals) => s.getEnumOrdinal(se.value)).toSet
  val meets :(Int) => Boolean =
    if (accepted.isEmpty) (i :Int) => true
    else accepted.contains(_)
  override def skip(decoder: Decoder) { decoder.readEnum() }
  def read(decoder: Decoder): Option[Int] = {
    val value = decoder.readEnum()
    if (!meets(value)) None
    else Some(value)
  }
}

class AvroLongReader(conds: Iterable[LongCond]) extends AvroPrimitiveReader {
  override def skip(decoder: Decoder) { decoder.readLong() }
  def read(decoder: Decoder): Option[Long] = {
    val value = decoder.readLong()
    if (conds.exists(!_.meets(value))) None
    else Some(value)
  }
}

class AvroFloatReader(conds: Iterable[DoubleCond]) extends AvroPrimitiveReader {
  override def skip(decoder: Decoder) { decoder.readFloat() }
  def read(decoder: Decoder): Option[Float] = {
    val value = decoder.readFloat()
    if (conds.exists(!_.meets(value))) None
    else Some(value)
  }
}

class AvroDoubleReader(conds: Iterable[DoubleCond]) extends AvroPrimitiveReader {
  override def skip(decoder: Decoder) { decoder.readDouble() }
  def read(decoder: Decoder): Option[Double] = {
    val value = decoder.readDouble()
    if (conds.exists(!_.meets(value))) None
    else Some(value)
  }
}

class BasicAvroPrimitiveReader(reader: ((Decoder) => Option[Any])) extends AvroPrimitiveReader {
  def read(decoder: Decoder) = reader(decoder)
}

class StringAvroPrimitiveReader extends AvroPrimitiveReader {
  private var utf8Buf = new Utf8
  def read(decoder: Decoder) = {
    utf8Buf = decoder.readString(utf8Buf)
    Some(utf8Buf)
  }
}

class BytesAvroPrimitiveReader extends AvroPrimitiveReader {
  private var byteBuf: ByteBuffer = null
  def read(decoder: Decoder) = {
    byteBuf = decoder.readBytes(byteBuf)
    Some(byteBuf)
  }
}
